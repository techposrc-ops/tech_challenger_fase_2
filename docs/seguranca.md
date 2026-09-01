# Segurança e dados sensíveis

## Credenciais

O projeto não utiliza chaves fixas no código. A autenticação local usa Application Default
Credentials e os serviços na GCP usam contas de serviço. Arquivos `.env`, credenciais JSON,
`terraform.tfvars`, estados e planos Terraform permanecem fora do Git.

## Permissões

- O bucket utiliza acesso uniforme e bloqueio de acesso público.
- O Cloud Run não possui permissão para `allUsers`.
- O Scheduler chama o produtor com token OIDC.
- Batch e Streaming acessam somente o bucket do Data Lake.
- A conta de Streaming pode gerar tokens somente para ela mesma.
- As contas possuem funções separadas para Batch, Streaming e orquestração.

## Dados

Os arquivos raw são públicos e vieram da Base dos Dados/INEP. Mesmo assim, identificadores de
alunos devem ser tratados como pseudônimos: não devem aparecer em logs, mensagens de erro ou
painéis públicos. As camadas geradas e os rejeitados ficam fora do Git.

## Logs e respostas

Os logs registram identificador da execução, status e contagens, sem registrar linhas completas.
Erros detalhados ficam no Cloud Logging. O produtor retorna uma mensagem genérica ao cliente para
não expor detalhes internos do Kafka ou da rede.

## Revisão antes da entrega

Antes de publicar uma nova versão:

1. executar `git status` e conferir arquivos não rastreados;
2. não adicionar `.env`, `terraform.tfvars`, estados, planos ou credenciais;
3. confirmar que não existe membro `allUsers` nas políticas IAM;
4. revisar os arquivos raw antes de incluir novas fontes;
5. manter testes e dados processados apenas no ambiente local.
