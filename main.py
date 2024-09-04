import requests
from datetime import datetime
import base64
import json
import time

class APIClient:
    def __init__(self, client_id, client_secret, url_base, talk_dic):
        self.client_id = client_id
        self.client_secret = client_secret
        self.url_base = url_base
        self.talk_dic = talk_dic
        self.access_token = None

    def get_access_token(self):
        # Requisita o token de acesso
        url_token = f"{self.url_base}/oauth/token"
        payload = {"grant_type": "client_credentials"}
        headers = {
            "accept": "application/json",
            "Authorization": f"Basic {base64.b64encode(bytes(f'{self.client_id}:{self.client_secret}', 'UTF-8')).decode('UTF-8')}",
            "content-type": "application/x-www-form-urlencoded"
        }
        response = requests.post(url_token, data=payload, headers=headers)
        data = response.json()
        self.access_token = data['access_token']

    def data_formatada(self):
        # Formata a data no padrão requerido
        return f"{datetime.now().date()}T00:00:00"

    def request_data(self, query_id, query_name):
        # Realiza a requisição da API
        if not self.access_token:
            self.get_access_token()

        url = f"{self.url_base}/live-subscriptions"
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "content-type": "application/json"
        }
        body = {
            "queries": [
                {
                    "id": query_id,
                    "metadata": {"name": query_name},
                    "params": {},
                    "filters": {"range": {"from": self.data_formatada()}}
                }
            ]
        }
        response = requests.post(url, headers=headers, json=body)
        return response.json()

    def process_response(self, response, keys_map):
        # Processa e soma os dados do status de usuários
        data = json.loads(response['_links']['stream']['href'])
        em_pausa = logados = disponivel = em_atendimento = 0

        for item in data['result']:
            nome = item["_key"]
            valor = int(item["_value"])

            if nome in keys_map['em_pausa']:
                em_pausa += valor
            elif nome == "_total":
                logados += valor
            elif nome == "available":
                disponivel += valor
            elif nome in keys_map['em_atendimento']:
                em_atendimento += valor

        return em_pausa, logados, disponivel, em_atendimento

    def get_fila(self):
        # Extrai informações da fila de espera
        response_fila = self.request_data(self.talk_dic['id_fila'], self.talk_dic['nome_fila'])
        data = json.loads(response_fila['_links']['stream']['href'])
        return data['result'][0]['_value']

class IntegracaoDavita:
    def __init__(self, api_client):
        self.api_client = api_client

    def enviar_dados(self, fila, em_pausa, logados, disponivel, em_atendimento):
        # Envia os dados para a API da DaVita
        url_request = "http://192.168.5.62:8091/BCMS_WEB/api/v1/Integracao/ReportTempoReal/Gravar"
        data_hoje = self.api_client.data_formatada()

        body = json.dumps({
            "Operacao": {"Constante": "RIO_CORPORATE"},
            "DataRegistro": data_hoje,
            "DataEvento": data_hoje,
            "Grupo": "12",
            "NomeGrupo": "DAVITA",
            "ChamadaEspera": fila,
            "NivelServico": "0",
            "Logado": logados,
            "Disponivel": disponivel,
            "Acd": em_atendimento,
            "Acw": em_pausa,
            "Aux": "0",
            "SaidaRamal": "0",
            "Outro": "0",
            "ChamadaAntiga": "0",
            "Itens": []
        })

        headers = {
            'content-type': "application/json",
            'cache-control': "no-cache"
        }
        response = requests.post(url_request, data=body, headers=headers)
        print(f"Davita Request - {response.status_code} - {body}")

def main():
    client_id = '591c97684fa54e3790897fdd0ecafac1'
    client_secret = 'ovcnLZ_V4uq4ijYzewn2n3R2RqhYj-PA5azGFdG20W3p4vT7BB0YyaKArxwIM_KVYqOVwMqLasfEjy2pMchDVg'
    url_base = 'https://davitabr.talkdeskid.com'
    talk_dic = {
        # Dicionário de dados relevantes
        "nome_fila": "live_contacts_in_queue",
        "id_fila": "2fbf3f809fca4352a5b458da347a6f9f",
        "nome_live_users": "live_users_logged_in_by_status",
        "id_live_users": "2d3f540ccecb4776902bd317fe2e411d"
    }

    keys_map = {
        'em_pausa': ["away", "away_ambulatrio", "away_ativo", "away_backoffice"],
        'em_atendimento': ["after_call_work", "busy"]
    }

    # Cria instâncias de cliente API e integração
    api_client = APIClient(client_id, client_secret, url_base, talk_dic)
    integracao_davita = IntegracaoDavita(api_client)

    while True:
        try:
            # Requisição de dados
            em_pausa, logados, disponivel, em_atendimento = api_client.process_response(api_client.request_data(talk_dic['id_live_users'], talk_dic['nome_live_users']), keys_map)
            fila = api_client.get_fila()

            # Envio de dados para API externa
            integracao_davita.enviar_dados(fila, em_pausa, logados, disponivel, em_atendimento)

            time.sleep(15)
        except Exception as e:
            print(f"Erro: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
