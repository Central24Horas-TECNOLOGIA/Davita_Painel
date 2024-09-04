import requests
from datetime import datetime
import base64
import json
import time
def data_formatada():
    # Obtém a data de hoje
    data_hoje = datetime.now().date()

    # Cria a string no formato desejado
    data_formatada = f"{data_hoje}T00:00:00"

    return data_formatada

def Request(data_hoje,Fila,em_pausa,logados,disponivel,em_atendimento):
    # URL do Servidor
    url_request = "http://192.168.5.62:8091/BCMS_WEB/api/v1/Integracao/ReportTempoReal/Gravar"

    try:
        '''REQUEST VOZ '''
        DAVITA = '{"Operacao": {"Constante": "RIO_CORPORATE"}, "DataRegistro": "' + f'{data_hoje}' + '", "DataEvento": "' + f'{data_hoje}' + '", "Grupo": "12", "NomeGrupo": "DAVITA", "ChamadaEspera": "' + f'{Fila}' + '", "NivelServico": "0", "Logado": "' + f'{logados}' + '", "Disponivel": "' + f'{disponivel}' + '", "Acd": "' + f'{em_atendimento}' + '", "Acw": "' + f'{em_pausa}' + '", "Aux": "0", "SaidaRamal": "0", "Outro": "0", "ChamadaAntiga": "0",\"Itens\": []}'

        headers = {
          'content-type': "application/json",
          'cache-control': "no-cache",
          'postman-token': "9e904e79-6ef2-4feb-416c-e1c2b410be09"
        }

        response_davita= requests.request("POST", url_request, data=DAVITA, headers=headers)

        print(f"Davitta Request - {response_davita.status_code}  - {DAVITA}")
        print(" ")

    except Exception as A:
            print(A)

#Credenciais do Cliente (Disponível na UI da Talkdesk)
Client_ID_DAVITA = '591c97684fa54e3790897fdd0ecafac1'
Client_Secret_DAVITA = 'ovcnLZ_V4uq4ijYzewn2n3R2RqhYj-PA5azGFdG20W3p4vT7BB0YyaKArxwIM_KVYqOVwMqLasfEjy2pMchDVg'

# DICIONÁRIO
TALK_DIC = {
    "nome_fila": "live_contacts_in_queue",
    "id_fila": "2fbf3f809fca4352a5b458da347a6f9f",
    "nome_tempo_atendimento": "avg_handle_time",
    "id_tempo_atendimento": "35267ca0b4c6410895ddd45ad4043a0e",
    "nome_nivel_servico": "service_level",
    "id_nivel_serivco": "ab8e693f64d741e081d117197929c2d9",
    "nome_live_users": "live_users_logged_in_by_status",
    "id_live_users": "2d3f540ccecb4776902bd317fe2e411d",
    "nome_Tempo__Médio_Espera": "avg_wait_time_by_ring_group",
    "id_Tempo_Médio_Espera": "7c9178ff86eb4b51ad86f75f3fc5cfe4",
    "nome_Maior_Tempo_Espera": "longest_wait_time_by_ring_group",
    "id_Maior_Tempo_Espera": "645cb1fe79a244a9a151592453d66c78",
    "nome_ligacoes_atendidas": "answered_contacts",
    "id_ligacoes_atendidas": "4e9788986d6845cd8f0d25d31f849bdf",
    "nome_ligacoes_perdidas": "missed_contacts",
    "id_ligacoes_perdidas": "91d2e348dbd64c4a8dde729356575c5d",
    "nome_ligacoes_total": "inbound_contacts",
    "id_ligacoes_total": "b7c50e0ae810452181a1d7c7f14cf8ba",
}

i = 0
while i == 0:
            try:
                #Requisitando a autorização através da credencial do client
                Autorizacao_DAVITA = base64.b64encode(bytes(f'{Client_ID_DAVITA}:{Client_Secret_DAVITA}',"UTF-8"))

                # Requisitando o Tolken através da autorização
                URL_API = "https://davitabr.talkdeskid.com/oauth/token"
                payload = {"grant_type": "client_credentials"}
                headers = {
                    "accept": "application/json",
                    "Authorization": "Basic NTkxYzk3Njg0ZmE1NGUzNzkwODk3ZmRkMGVjYWZhYzE6b3ZjbkxaX1Y0dXE0aWpZemV3bjJuM1IyUnFoWWotUEE1YXpHRmRHMjBXM3A0dlQ3QkIwWXlhS0FyeHdJTV9LVllxT1Z3TXFMYXNmRWp5MnBNY2hEVmc=",
                    "content-type": "application/x-www-form-urlencoded"
                }
                response = requests.post(URL_API, data=payload, headers=headers)
                data = json.loads(response.text)
                access_token = data['access_token']

                # Usando o Tolken para extrair as informações
                url = "https://api.talkdeskapp.com/live-subscriptions"
                headers = {
                    "accept": "application/json",
                    "Authorization": f"Bearer {access_token}",
                    "content-type": "application/json"
                }

                #StatusUsários
                body_status_users = {
                            "queries": [
                                {
                                    "id": f"{TALK_DIC['id_live_users']}",
                                    "metadata": {
                                        "name": f"{TALK_DIC['nome_live_users']}"
                                    },
                                    "params": {},
                                    "filters": {"range": {"from": f"{data_formatada()}"}},
                                }
                            ]
                        }
                response_status_users = requests.post(url, headers=headers, json=body_status_users)
                data = json.loads(response_status_users.text)
                stream_href_url_status_users = data['_links']['stream']['href']
                with (requests.get(stream_href_url_status_users, stream=True) as response_status_users):
                    for chunk in response_status_users.iter_content(chunk_size=1000):
                        print(chunk)
                        if chunk:
                            Result_fila = chunk.decode("utf-8")
                            dados_json = json.loads(Result_fila.split("data:")[1])
                            em_pausa = 0
                            logados = 0
                            disponivel = 0
                            em_atendimento = 0

                            # Extrair os resultados e somar as quantidades desejadas
                            for item in dados_json["result"]:
                                nome = item["_key"]
                                valor = int(item["_value"])  # Converte o valor para inteiro

                                if nome == "away" or nome == "away_ambulatrio" or nome == "away_ativo" or nome == "away_backoffice" or nome == "away_banheiro" or nome == "away_descanso" or nome == "away_lanche" or nome == "away_reunio":
                                    em_pausa += valor
                                elif nome == "_total":
                                    logados += valor
                                elif nome == "available":
                                    disponivel += valor
                                elif nome == "after_call_work" or nome == "busy":
                                    em_atendimento += valor
                            pass
                            break

                #Fila
                body_fila = {
                    "queries": [
                        {
                            "id": f"{TALK_DIC['id_fila']}",
                            "metadata": {
                                "name": f"{TALK_DIC['nome_fila']}"
                            },
                            "params": {},
                            "filters": {"range": {"from": f"{data_formatada()}"}},
                        }
                    ]
                }
                response_fila = requests.post(url, headers=headers, json=body_fila)
                data = json.loads(response_fila.text)
                stream_href_url_fila = data['_links']['stream']['href']
                with requests.get(stream_href_url_fila, stream=True) as response_fila:
                    for chunk in response_fila.iter_content(chunk_size=128):
                        if chunk:
                            Result_fila = chunk.decode("utf-8")
                            indice_result = Result_fila.find('"result":')
                            substring = Result_fila[indice_result:]
                            indice_colchete_aberto = substring.find('[')
                            indice_colchete_fechado = substring.find(']')
                            resultado_substring = substring[indice_colchete_aberto:indice_colchete_fechado + 1]
                            resultado_json = json.loads(resultado_substring)
                            Fila = resultado_json[0]['_value']
                            break

                print(f'Em pausa: {em_pausa}')
                print(f'Logados: {logados}')
                print(f'Disponível: {disponivel}')
                print(f"Em atendimento:{em_atendimento}")
                print(f"Fila: {Fila}")

                data_hoje = data_formatada()
                # Aqui você poderia chamar a função Request() com os dados que coletou
                #Request(data_hoje,Fila,em_pausa,logados,disponivel,em_atendimento)
                time.sleep(15)
                # Reiniciar o temporizador de espera
                tempo = 30
            except:
                tempo = 30
                # print("Erro:", e)
                print("Aguardando", tempo, "segundos antes de tentar novamente...")
                time.sleep(tempo)
                # Dobrando o tempo de espera a cada falha consecutiva
                tempo += 60
