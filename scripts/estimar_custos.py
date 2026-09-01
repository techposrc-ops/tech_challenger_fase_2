"""Estimativa simples dos principais custos usados no projeto."""

import argparse
# As variáveis foram baseadas nas páginas oficiais de preços do Google Cloud para a região us-central1
PRECO_DCU_DATAPROC_SERVERLESS = 0.06
PRECO_SHUFFLE_GIB_HORA = 0.000054795
PRECO_VM_E2_STANDARD_2_HORA = 0.06701142
PRECO_GERENCIAMENTO_DATAPROC_VCPU_HORA = 0.01
PRECO_KAFKA_DCU_HORA = 0.09
PRECO_KAFKA_STORAGE_GIB_HORA = 0.000232877
PRECO_STORAGE_STANDARD_GIB_MES = 0.02


def calcular_batch(milli_dcu_segundos, shuffle_gib_segundos):
    dcu_horas = milli_dcu_segundos / 1000 / 3600
    shuffle_gib_horas = shuffle_gib_segundos / 3600
    return (
        dcu_horas * PRECO_DCU_DATAPROC_SERVERLESS
        + shuffle_gib_horas * PRECO_SHUFFLE_GIB_HORA
    )


def calcular_kafka(horas, vcpus=3, memoria_gib=3):
    dcus = vcpus * 0.6 + memoria_gib * 0.1
    armazenamento_local = vcpus * 100
    return horas * (
        dcus * PRECO_KAFKA_DCU_HORA
        + armazenamento_local * PRECO_KAFKA_STORAGE_GIB_HORA
    )


def calcular_dataproc_cluster(horas, quantidade_vms=3, vcpus_por_vm=2):
    custo_vms = quantidade_vms * PRECO_VM_E2_STANDARD_2_HORA
    total_vcpus = quantidade_vms * vcpus_por_vm
    gerenciamento = total_vcpus * PRECO_GERENCIAMENTO_DATAPROC_VCPU_HORA
    return horas * (custo_vms + gerenciamento)


def calcular_storage_mes(volume_gib):
    return volume_gib * PRECO_STORAGE_STANDARD_GIB_MES


def ler_argumentos():
    analisador = argparse.ArgumentParser(
        description="Estima custos do teste na GCP em USD.")
    analisador.add_argument("--milli-dcu-segundos",
                            type=float, default=2_002_650)
    analisador.add_argument("--shuffle-gib-segundos",
                            type=float, default=202_800)
    analisador.add_argument("--horas-kafka", type=float, default=1)
    analisador.add_argument("--horas-dataproc-streaming",
                            type=float, default=0.5)
    analisador.add_argument("--volume-storage-gib", type=float, default=0.13)
    return analisador.parse_args()


def principal():
    argumentos = ler_argumentos()
    batch = calcular_batch(
        argumentos.milli_dcu_segundos,
        argumentos.shuffle_gib_segundos,
    )
    kafka = calcular_kafka(argumentos.horas_kafka)
    dataproc_streaming = calcular_dataproc_cluster(
        argumentos.horas_dataproc_streaming
    )
    storage_mes = calcular_storage_mes(argumentos.volume_storage_gib)
    total_teste = batch + kafka + dataproc_streaming

    print(f"Batch Dataproc Serverless: US$ {batch:.4f}")
    print(f"Kafka gerenciado:          US$ {kafka:.4f}")
    print(f"Dataproc Streaming:        US$ {dataproc_streaming:.4f}")
    print(f"Total aproximado do teste: US$ {total_teste:.4f}")
    print(f"Cloud Storage por mês:     US$ {storage_mes:.4f}")
    print("Não inclui impostos, câmbio, rede e operações de armazenamento.")


if __name__ == "__main__":
    principal()
