# Qualidade dos dados

## Regras aplicadas

A camada Silver valida as seis tabelas do projeto. As verificações são:

- chaves obrigatórias nulas;
- percentuais menores que 0 ou maiores que 100;
- registros duplicados pelas chaves de cada tabela;
- alunos sem município correspondente;
- municípios sem UF correspondente.

## Registros inválidos

Os registros inválidos não são apagados. Eles são gravados em:

```text
silver/_rejeitados/<nome_da_tabela>
```

Cada registro possui `_registro_valido` e `_motivo_invalido`. Os registros válidos seguem para a
camada Gold.

## Relatório por execução

As métricas são gravadas em Parquet:

```text
silver/_qualidade/id_execucao=<identificador>
```

O relatório contém tabela, total de registros, válidos, rejeitados, duplicidades, chaves nulas,
percentuais inválidos, erros de relacionamento, identificador e data da execução.

## Erros críticos

O pipeline para antes da Gold quando encontra:

- alguma das seis tabelas ausente;
- tabela sem registros válidos;
- alunos sem município correspondente;
- municípios sem UF correspondente.

Antes da interrupção, o relatório é salvo e um log estruturado `qualidade_dados` é emitido.
