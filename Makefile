SHELL := /bin/bash

# Carrega variaveis locais (opcional). Assim KEYCLOAK_HOST_PORT definido no
# .env flui para o compose e para os alvos de token automaticamente.
-include .env
COMPOSE ?= docker compose
ENDPOINT ?= http://localhost:4566
PSQL ?= psql "postgres://wager:wager@localhost:5432/wager?sslmode=disable"

.DEFAULT_GOAL := help

.PHONY: help up down reset logs ps queues build run fmt vet test race check psql token token-payload

help: ## Lista os alvos disponíveis
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

up: ## Sobe a infraestrutura (postgres + ministack + keycloak)
	$(COMPOSE) up -d postgres ministack keycloak

down: ## Derruba tudo (mantém volumes)
	$(COMPOSE) down

reset: ## Derruba tudo e REMOVE volumes (estado limpo)
	$(COMPOSE) down -v

logs: ## Segue os logs de todos os serviços
	$(COMPOSE) logs -f

ps: ## Lista containers
	$(COMPOSE) ps

queues: ## Lista as filas SQS provisionadas no emulador
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
	aws --endpoint-url $(ENDPOINT) sqs list-queues

build: ## Compila todos os pacotes
	go build ./...

run: ## Executa a API localmente (usa variáveis do shell/.env)
	go run ./cmd/api

fmt: ## Formata e checa formatação
	gofmt -l -w .

vet: ## go vet
	go vet ./...

test: ## Testes
	go test ./...

race: ## Testes com detector de corrida
	go test -race ./...

check: fmt vet test ## fmt + vet + test

psql: ## Abre um psql no Postgres do Compose
	$(PSQL)

CLIENT ?= provider-a
CLIENT_SECRET ?= provider-a-secret
KEYCLOAK_HOST_PORT ?= 8080
KEYCLOAK_URL ?= http://localhost:$(KEYCLOAK_HOST_PORT)

token: ## Obtem um access_token via client_credentials (CLIENT=provider-a por padrao)
	@curl -s -X POST $(KEYCLOAK_URL)/realms/wager/protocol/openid-connect/token \
	  -d grant_type=client_credentials \
	  -d client_id=$(CLIENT) \
	  -d client_secret=$(CLIENT_SECRET) | jq -r .access_token

token-internal: ## Obtem um access_token do servico interno
	@$(MAKE) --no-print-directory token CLIENT=internal-service CLIENT_SECRET=internal-secret

token-payload: ## Decodifica o payload do token (usa CLIENT/CLIENT_SECRET)
	@python3 -c "import sys,base64,json;p=sys.argv[1].split('.')[1];p+='='*(-len(p)%4);print(json.dumps(json.loads(base64.urlsafe_b64decode(p)),indent=2,ensure_ascii=False))" "$$(curl -s -X POST $(KEYCLOAK_URL)/realms/wager/protocol/openid-connect/token -d grant_type=client_credentials -d client_id=$(CLIENT) -d client_secret=$(CLIENT_SECRET) | jq -r .access_token)"

.PHONY: migrate-up migrate-down migrate-version schema-check

migrate-up: ## Aplica as migrations (usa POSTGRES_DSN ou -dsn)
	go run ./cmd/migrate up

migrate-down: ## Reverte todas as migrations
	go run ./cmd/migrate down

migrate-version: ## Mostra a versão atual do schema
	go run ./cmd/migrate version

schema-check: ## Prova as constraints/triggers num banco descartável
	./docs/verify/01_schema.sh

TEST_POSTGRES_DSN ?= postgres://wager:wager@localhost:5432/wager_test?sslmode=disable

.PHONY: test-integration test-all test-oidc test-broker test-consumer demo

SQS_ENDPOINT ?= http://localhost:4566

test-integration: ## Testes de integracao (Postgres, Keycloak e SQS REAIS, todos os pacotes)
	TEST_POSTGRES_DSN="$(TEST_POSTGRES_DSN)" \
	TEST_OIDC_BASE_URL="$(KEYCLOAK_URL)" \
	TEST_OIDC_REQUIRED=1 \
	TEST_SQS_ENDPOINT="$(SQS_ENDPOINT)" \
	TEST_SQS_REQUIRED=1 \
	go test -tags integration -race ./...

test-oidc: ## Testes de autenticacao contra o Keycloak real
	TEST_POSTGRES_DSN="$(TEST_POSTGRES_DSN)" TEST_OIDC_BASE_URL="$(KEYCLOAK_URL)" TEST_OIDC_REQUIRED=1 \
	go test -tags integration -race ./internal/auth/...

test-broker: ## Testes de mensageria contra o SQS emulado real
	TEST_SQS_ENDPOINT="$(SQS_ENDPOINT)" TEST_SQS_REQUIRED=1 \
	go test -tags integration -race ./internal/broker/...

test-consumer: ## Testes do consumidor SQS (Postgres e SQS reais)
	TEST_POSTGRES_DSN="$(TEST_POSTGRES_DSN)" \
	TEST_SQS_ENDPOINT="$(SQS_ENDPOINT)" \
	TEST_SQS_REQUIRED=1 \
	go test -tags integration -race ./internal/worker/...

DEMO_BASE ?= http://localhost:8081

demo: ## Roteiro dos cenarios de concorrencia e recuperacao (enunciado secao 13)
	BASE="$(DEMO_BASE)" \
	BASES="8081 8082 8083" \
	KEYCLOAK_URL="$(KEYCLOAK_URL)" \
	SQS_ENDPOINT="$(SQS_ENDPOINT)" \
	./docs/demo/demonstrate.sh

test-all: check test-integration ## fmt + vet + unitarios + integracao

