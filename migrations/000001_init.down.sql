-- 000001_init.down.sql
-- Reversão completa da migration 0001. CASCADE remove as FKs e os triggers
-- dependentes na ordem correta, independentemente da ordem de execução.

DROP TABLE IF EXISTS wallet_ledger_entries CASCADE;
DROP TABLE IF EXISTS outbox_events        CASCADE;
DROP TABLE IF EXISTS inbox_messages       CASCADE;
DROP TABLE IF EXISTS wager_transactions   CASCADE;
DROP TABLE IF EXISTS wallets              CASCADE;

DROP FUNCTION IF EXISTS wallet_ledger_entries_immutable();
