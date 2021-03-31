import singer
import json
import urllib.request
from datetime import datetime, timezone


class TapAgregados():
    
    
    def __init__(self):
        self.grupos = []
        self.agregados = []
        self.metadados = []
        self.agregado_variaveis = []
        self.agregado_classificacao = []

        self.LOGGER = singer.get_logger()

    def agregados_fetch_data(self):
        with urllib.request.urlopen('https://servicodados.ibge.gov.br/api/v3/agregados') as response:
            agregados_result = response.read().decode('utf-8')
            list_records = json.loads(agregados_result)

            now = datetime.now(timezone.utc).isoformat()
            for record in list_records:
                self.grupos.append({'id': record['id'],
                                    'nome': record['nome'],
                                    'timestamp': now
                                    })
                agregados_records = record['agregados']
                for agregado in agregados_records:
                    self.agregados.append({'id_grupo': record['id'],
                                           'id': agregado['id'],
                                           'nome': agregado['nome'],
                                           'timestamp': now
                                           })
                break

    def metadados_fetch_data(self):
        for agregado in self.agregados:
            with urllib.request.urlopen('https://servicodados.ibge.gov.br/api/v3/agregados/{}/metadados'.format(agregado['id'])) as response:
                agregados_result = response.read().decode('utf-8')
                record = json.loads(agregados_result)

            self.LOGGER.info('agregado[{}]'.format(agregado['id']))
            now = datetime.now(timezone.utc).isoformat()

            self.metadados.append({ 'id': record['id'],
                                    'nome': record['nome'],
                                    'URL': record['URL'],
                                    'pesquisa': record['pesquisa'],
                                    'assunto': record['assunto'],
                                    'periodicidade_frequencia': record['periodicidade']['frequencia'],
                                    'periodicidade_inicio': record['periodicidade']['inicio'],
                                    'periodicidade_fim': record['periodicidade']['inicio'],
                                    'nivel_territorial_administrativo': record['nivelTerritorial']['Administrativo'],
                                    'nivel_territorial_especial': record['nivelTerritorial']['Especial'],
                                    'nivel_territorial_IBGE': record['nivelTerritorial']['IBGE'],
                                    'timestamp': now
                                  })

            variaveis = record['variaveis']
            for variavel in variaveis:
                self.agregado_variaveis.append({'id_agregado': record['id'],
                                                'id': variavel['id'],
                                                'nome': variavel['nome'],
                                                'unidade': variavel['unidade'],
                                                'sumarizacao': variavel['sumarizacao'],
                                                'timestamp': now
                                               })

            classificacoes = record['classificacoes']
            for classificacao in classificacoes:
                self.agregado_classificacao.append({'id_agregado': record['id'],
                                                    'id': classificacao['id'],
                                                    'nome': classificacao['nome'],
                                                    'sumarizacao_status': classificacao['sumarizacao']['status'],
                                                    'sumarizacao_excecao': classificacao['sumarizacao']['excecao'],
                                                    'timestamp': now
                                                   })

    def write_grupos(self):
        schema = {
            'properties': {
                "id": {"type": "string"},
                "nome": {"type": "string"},
                "timestamp": {"type": "string", "format": "date-time"},
            }
        }
        singer.write_schema('grupo_agregados', schema, ['id'])
        singer.write_records('grupo_agregados', self.grupos)

    def write_agregados(self):
        schema = {
            'properties': {
                "id_grupo": {"type": "string"},
                "id": {"type": "string"},
                "nome": {"type": "string"},
                "timestamp": {"type": "string", "format": "date-time"},
            }
        }
        singer.write_schema('agregados', schema, ['id'])
        singer.write_records('agregados', self.agregados)

    def write_metadados(self):
        schema = {
            'properties': {
                "id": {"type": "integer"},
                "nome": {"type": "string"},
                "URL": {"type": "string"},
                "pesquisa": {"type": "string"},
                "assunto": {"type": "string"},
                "periodicidade_frequencia": {"type": "string"},
                "periodicidade_inicio": {"type": "integer"},
                "periodicidade_fim": {"type": "integer"},
                "timestamp": {"type": "string", "format": "date-time"},
            }
        }
        singer.write_schema('metadados_agregados', schema, ['id'])
        singer.write_records('metadados_agregados', self.metadados)

    def run(self):
        self.agregados_fetch_data()
        self.metadados_fetch_data()
        self.write_grupos()
        self.write_agregados()
        self.write_metadados()


if __name__ == '__main__':
    tagg = TapAgregados()
    tagg.run()
