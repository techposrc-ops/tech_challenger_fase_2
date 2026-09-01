# Dicionário das fontes integradas

Dataset BigQuery: `basedosdados.br_inep_avaliacao_alfabetizacao`.

| Tabela | Chave de negócio | Papel analítico |
|---|---|---|
| `uf` | ano, sigla_uf, série, rede | Indicadores de desempenho estaduais |
| `meta_alfabetizacao_brasil` | ano, rede | Metas e resultados nacionais |
| `meta_alfabetizacao_uf` | ano, sigla_uf, rede | Metas estaduais |
| `meta_alfabetizacao_municipio` | ano, id_municipio, rede | Metas municipais |
| `municipio` | ano, id_municipio, série, rede | Desempenho territorial municipal |
| `alunos` | ano, id_municipio, id_escola, id_aluno | Microdados educacionais |

As metas permanecem nas colunas `meta_alfabetizacao_2024` a
`meta_alfabetizacao_2030`. Na Gold, a coluna `meta_do_ano` seleciona a meta correspondente ao ano
do resultado. A comparação também cria:

- `diferenca_para_meta`: taxa oficial menos a meta do ano;
- `situacao_meta`: `Atingida`, `Não atingida` ou `Sem meta`.

## Produto municipal integrado

`gold/indicadores_municipio` combina:

- taxa e média de proficiência da tabela `municipio`;
- meta e participação da tabela `meta_alfabetizacao_municipio`;
- quantidade de alunos presentes e alfabetizados da tabela `alunos`;
- taxa de alfabetização calculada pelos microdados e média de proficiência;
- diferença para a meta e indicador de meta atingida.

## Produtos por UF e Brasil

- `gold/indicadores_uf`: taxa oficial, proficiência, participação e comparação com a meta por UF;
- `gold/indicadores_brasil`: taxa oficial, participação e comparação com a meta nacional.

Os identificadores escolares são fictícios conforme o catálogo da fonte. Nenhuma informação é
marcada pela Base dos Dados como sensível, mas a Gold evita publicar identificadores individuais.
