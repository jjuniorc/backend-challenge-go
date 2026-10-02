package usecase

// Códigos de falha estáveis e documentados.
//
// Eles são persistidos em wager_transactions.failure_code, enviados em
// WagerTransactionRejected e devolvidos no contrato HTTP. Uma vez publicados,
// não devem mudar de significado: o provedor usa o código para decidir se a
// entrada é corrigível ou definitiva.
//
// Convenção:
//   - prefixo WALLET_    -> problema na carteira (saldo, moeda, jogador);
//   - prefixo REFERENCE_ -> problema na transação referenciada;
//   - prefixo IDEMPOTENCY_ -> conflito de chave/conteúdo.
const (
	// Carteira
	CodeWalletNotFound          = "WALLET_NOT_FOUND"
	CodeWalletPlayerMismatch    = "WALLET_PLAYER_MISMATCH"
	CodeWalletCurrencyMismatch  = "WALLET_CURRENCY_MISMATCH"
	CodeWalletInsufficientFunds = "WALLET_INSUFFICIENT_FUNDS"
	CodeWalletBalanceOverflow   = "WALLET_BALANCE_OVERFLOW"
	CodeWalletAlreadyExists     = "WALLET_ALREADY_EXISTS"

	// Reversões: código DISTINTO de WALLET_INSUFFICIENT_FUNDS, exigido pelo
	// README para diferenciar uma aposta sem saldo de uma reversão inviável.
	CodeReversalInsufficientFunds = "REVERSAL_INSUFFICIENT_FUNDS"

	// Referência
	CodeReferenceNotFound          = "REFERENCE_NOT_FOUND"
	CodeReferenceNotProcessed      = "REFERENCE_NOT_PROCESSED"
	CodeReferenceAlreadyReversed   = "REFERENCE_ALREADY_REVERSED"
	CodeReferenceProviderMismatch  = "REFERENCE_PROVIDER_MISMATCH"
	CodeReferencePlayerMismatch    = "REFERENCE_PLAYER_MISMATCH"
	CodeReferenceWalletMismatch    = "REFERENCE_WALLET_MISMATCH"
	CodeReferenceCurrencyMismatch  = "REFERENCE_CURRENCY_MISMATCH"
	CodeReferenceRoundMismatch     = "REFERENCE_ROUND_MISMATCH"
	CodeReferenceAmountMismatch    = "REFERENCE_AMOUNT_MISMATCH"
	CodeReferenceKindNotRefundable = "REFERENCE_KIND_NOT_REFUNDABLE"
	CodeReferenceKindNotReversible = "REFERENCE_KIND_NOT_REVERSIBLE"

	// Idempotência
	CodeIdempotencyKeyReused   = "IDEMPOTENCY_KEY_REUSED"
	CodeIdempotencyKeyMismatch = "IDEMPOTENCY_KEY_MISMATCH"

	// Inbox (entrada por broker). São a identidade da MENSAGEM, não da
	// operação: um conflito aqui significa reentrega com conteúdo diferente.
	CodeInboxPayloadHashMismatch = "INBOX_PAYLOAD_HASH_MISMATCH"
	CodeInboxIncomplete          = "INBOX_INCOMPLETE"

	// Referência pendente esgotada: número máximo de tentativas OU TTL da
	// política. É o desfecho terminal da retomada (README: "Quando esgotado,
	// finalize como REJECTED").
	CodeReferenceTimeout = "REFERENCE_TIMEOUT"
)
