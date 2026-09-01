# Notebooks

- `analise_integrada.ipynb`: integração de resultados, metas municipais e microdados de alunos.
- `exploracao_bronze.ipynb`: inventário, esquemas, metadados, nulos e duplicidades das seis
  fontes armazenadas na Bronze.

Execute primeiro a pipeline a partir da raiz do projeto:

```powershell
python -m alfabetizacao.cli --source-dir data/sample --output-dir data
```

Depois instale e abra o ambiente de notebooks:

```powershell
python -m pip install -e ".[notebooks]"
python -m jupyter lab
```

O notebook localiza automaticamente a raiz do projeto, mesmo quando iniciado dentro da pasta
`notebooks`.
