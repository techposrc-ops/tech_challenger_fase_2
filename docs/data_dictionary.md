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

As metas originalmente aparecem em colunas de `meta_alfabetizacao_2024` a
`meta_alfabetizacao_2030`. Na Gold elas são normalizadas em duas colunas:

- `ano_meta`;
- `meta_alfabetizacao`.

## Produto municipal integrado

`gold/indicador_municipio_ano` combina:

- taxa e média de proficiência da tabela `municipio`;
- meta e participação da tabela `meta_alfabetizacao_municipio`;
- quantidade de alunos e escolas da tabela `alunos`;
- presença, preenchimento, alfabetização e proficiência agregadas dos alunos;
- diferença para a meta e indicador de meta atingida.

Os identificadores escolares são fictícios conforme o catálogo da fonte. Nenhuma informação é
marcada pela Base dos Dados como sensível, mas a Gold evita publicar identificadores individuais.

