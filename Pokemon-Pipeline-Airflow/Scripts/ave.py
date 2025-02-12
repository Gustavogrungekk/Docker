class InteractionLogger:
    def log(self, interaction):
        self._log_to_dynamo(interaction)
        self._emit_cloudwatch_metrics({
            'PersonaUsed': interaction['persona'],
            'QueryRuntime': interaction['duration'],
            'ResultSetSize': interaction['result_size']
        })
    
    def _emit_cloudwatch_metrics(self, metrics):
        self.client.put_metric_data(
            Namespace='ChatBot',
            MetricData=[{
                'MetricName': key,
                'Value': value,
                'Unit': 'Count'
            } for key, value in metrics.items()]
        )

import json
import numpy as np
from openai import OpenAI
from sklearn.metrics.pairwise import cosine_similarity

class PersonaMatcher:
    def __init__(self):
        self.client = OpenAI()
        self.personas = self.load_personas()
        self.embed_model = "text-embedding-3-small"
        
    def load_personas(self):
        with open('data/personas.json') as f:
            return json.load(f)
    
    def get_embedding(self, text):
        response = self.client.embeddings.create(
            input=text,
            model=self.embed_model
        )
        return response.data[0].embedding
    
    def match_persona(self, user_input, threshold=0.82):
        # Gerar embedding da pergunta do usuário
        input_embed = np.array(self.get_embedding(user_input)).reshape(1, -1)
        
        # Calcular similaridade com cada persona
        similarities = []
        for persona in self.personas:
            if not persona["embedding"]:
                persona["embedding"] = self.get_embedding(
                    f"{persona['description']} {' '.join(persona['keywords'])}"
                )
            persona_embed = np.array(persona["embedding"]).reshape(1, -1)
            sim = cosine_similarity(input_embed, persona_embed)[0][0]
            similarities.append((persona, sim))
        
        # Encontrar melhor match
        best_match = max(similarities, key=lambda x: x[1])
        
        return best_match[0] if best_match[1] >= threshold else next(
            p for p in self.personas if p["id"] == "default"
        )

class QueryValidator:
    def __init__(self):
        with open('data/metadata.json') as f:
            self.metadata = json.load(f)
    
    def validate_access(self, query, persona):
        # Extrair tabelas usadas na query
        used_tables = self.extract_tables(query)
        
        # Verificar permissões
        for table in used_tables:
            if (persona["allowed_tables"] != ["*"] and 
                table not in persona["allowed_tables"]):
                raise PermissionError(
                    f"Persona {persona['name']} não tem acesso à tabela {table}"
                )
    
    def extract_tables(self, query):
        # Lógica simples para extração de tabelas
        return [tbl.split(".")[-1] 
                for tbl in query.upper().split("FROM ")[1].split(" ")[0].split(",")]

# app/backend/agent.py
import json
import logging
import pandas as pd
from typing import Dict, Any, Optional
from datetime import datetime

# Módulos internos
from .persona_matcher import PersonaMatcher
from ..connectors.athena_connector import AthenaHandler
from ..utils.query_validator import QueryValidator
from ..utils.logger import InteractionLogger
from ..config import constants

class QueryAgent:
    def __init__(self):
        """
        Inicializa o agente principal com todos os componentes necessários:
        - Sistema de matching de personas
        - Conector do Athena
        - Validador de queries
        - Sistema de logging
        """
        self.persona_matcher = PersonaMatcher()
        self.athena = AthenaHandler()
        self.validator = QueryValidator()
        self.logger = InteractionLogger()
        self.metadata = self._load_metadata()
        self.feedback_system = FeedbackAnalyzer()

    def _load_metadata(self) -> Dict[str, Any]:
        """Carrega os metadados do banco de dados"""
        try:
            with open(constants.METADATA_PATH) as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Erro ao carregar metadados: {str(e)}")
            return {}

    def process_query(self, user_input: str) -> Dict[str, Any]:
        """
        Fluxo principal de processamento de consultas:
        1. Seleção automática de persona
        2. Construção da query
        3. Validação e execução
        4. Formatação da resposta
        """
        start_time = datetime.now()
        
        try:
            # Passo 1: Seleção de persona
            selected_persona = self.persona_matcher.match_persona(user_input)
            
            # Passo 2: Construção da query
            query = self._build_query(user_input, selected_persona)
            
            # Passo 3: Validação
            self.validator.validate_query(query, selected_persona)
            
            # Passo 4: Execução
            results = self.athena.execute(query)
            
            # Passo 5: Formatação
            response = self._format_response(
                user_input=user_input,
                query=query,
                results=results,
                persona=selected_persona,
                exec_time=datetime.now() - start_time
            )

        except Exception as e:
            response = self._handle_error(e, user_input)
            results = pd.DataFrame()

        # Logging e feedback
        self._log_interaction(
            user_input=user_input,
            response=response,
            results=results,
            error=response.get('error')
        )

        return response

    def _build_query(self, user_input: str, persona: Dict[str, Any]) -> str:
        """Constroi a query SQL considerando a persona selecionada"""
        # Lógica complexa de construção de query aqui
        base_query = f"SELECT * FROM {self.metadata['main_table']}"
        
        # Aplica filtros baseados na persona
        if persona['id'] == 'financial_analyst':
            return f"{base_query} WHERE financial_data IS NOT NULL LIMIT 1000"
        elif persona['id'] == 'marketing_specialist':
            return f"{base_query} WHERE campaign_data IS NOT NULL LIMIT 1000"
        
        return f"{base_query} LIMIT 100"

    def _format_response(self, **kwargs) -> Dict[str, Any]:
        """Formata a resposta final com explicações"""
        return {
            'persona': kwargs['persona'],
            'query': kwargs['query'],
            'results': kwargs['results'],
            'execution_time': str(kwargs['exec_time']),
            'explanation': self._generate_explanation(
                kwargs['user_input'],
                kwargs['persona']
            ),
            'visualization_hint': self._suggest_visualization(kwargs['results']),
            'error': None
        }

    def _generate_explanation(self, question: str, persona: Dict) -> str:
        """Gera explicação natural da escolha da persona"""
        return (
            f"A persona **{persona['name']}** foi selecionada porque sua pergunta "
            f"sobre **{self._extract_key_terms(question)}** se alinha com a especialização "
            f"em: {persona['description']}"
        )

    def _suggest_visualization(self, data: pd.DataFrame) -> str:
        """Sugere o melhor tipo de visualização baseado nos dados"""
        if 'date' in data.columns:
            return "line"
        if 'category' in data.columns:
            return "bar"
        return "table"

    def _handle_error(self, error: Exception, user_input: str) -> Dict[str, Any]:
        """Trata erros e gera respostas adequadas"""
        logging.error(f"Erro ao processar: {user_input} - {str(error)}")
        
        return {
            'persona': None,
            'query': None,
            'results': pd.DataFrame(),
            'execution_time': None,
            'explanation': f"Erro ao processar sua solicitação: {str(error)}",
            'visualization_hint': None,
            'error': True
        }

    def _log_interaction(self, **kwargs):
        """Registra a interação completa"""
        log_data = {
            'timestamp': datetime.now().isoformat(),
            'user_input': kwargs['user_input'],
            'persona': kwargs['response']['persona'],
            'generated_query': kwargs['response']['query'],
            'result_stats': {
                'row_count': len(kwargs['results']),
                'columns': list(kwargs['results'].columns)
            },
            'error': kwargs.get('error'),
            'feedback': None
        }
        
        self.logger.log_interaction(log_data)
        self.feedback_system.analyze_feedback(log_data)

    def _extract_key_terms(self, text: str) -> str:
        """Extrai termos-chave para explicações"""
        # Implementação simplificada
        return ", ".join(text.split()[:3]) + "..."

class FeedbackAnalyzer:
    """Analisa feedback para melhorar o matching de personas"""
    def __init__(self):
        self.feedback_data = []

    def analyze_feedback(self, log_entry: Dict):
        """Processa feedback para ajustar modelos"""
        # Implementação completa incluiria:
        # - Atualização de embeddings de personas
        # - Ajuste de thresholds de matching
        # - Análise de erros recorrentes
        self.feedback_data.append(log_entry)
        logging.info(f"Novo feedback registrado: {log_entry['user_input']}")

if __name__ == "__main__":
    # Teste rápido do sistema
    agent = QueryAgent()
    test_query = "Qual foi o crescimento de receita no último trimestre?"
    response = agent.process_query(test_query)
    print(f"Persona selecionada: {response['persona']['name']}")
    print(f"Query gerada:\n{response['query']}")
    print(f"Explicação: {response['explanation']}")







# app/frontend/streamlit_app.py
import streamlit as st
from backend.agent import QueryAgent

def main():
    st.set_page_config(page_title="Athena Assistant", layout="wide")
    
    # Sidebar com seleção manual + auto-sugestão
    with st.sidebar:
        st.image("assets/logo.png")
        persona_options = load_personas()
        
        # Seleção manual
        selected_persona = st.selectbox(
            "Escolha a Persona:",
            options=persona_options,
            format_func=lambda x: x["name"],
            index=0  # Default como primeira opção
        )
        
        # Opção de auto-detecção
        auto_detect = st.toggle("Auto-detectar melhor persona", True)
        
        if auto_detect:
            st.info("Sistema irá sugerir ajustes quando necessário")

    # Chat principal
    user_input = st.chat_input("Faça sua pergunta de negócios...")
    
    if user_input:
        agent = QueryAgent()
        
        with st.spinner("Analisando..."):
            # Etapa rápida: usar persona selecionada
            initial_response = agent.quick_process(user_input, selected_persona)
            
            # Etapa paralela: verificar se há match melhor
            if auto_detect:
                suggested_persona = agent.suggest_better_persona(user_input, selected_persona)
                
                if suggested_persona and suggested_persona != selected_persona["id"]:
                    initial_response["suggestion"] = suggested_persona
        
        display_response(initial_response)

def display_response(response):
    # Exibir resposta principal
    with st.chat_message("assistant"):
        st.markdown(response["answer"])
        st.code(response["query"])
        
        # Mostrar sugestão se houver
        if "suggestion" in response:
            st.warning(f"💡 Sugerimos usar a persona **{response['suggestion']['name']}** para perguntas deste tipo!")
            if st.button("Aplicar sugestão automaticamente"):
                st.session_state.persona = response["suggestion"]
                st.rerun()



# app/backend/agent.py
import time
from functools import lru_cache

class QueryAgent:
    def __init__(self):
        self.persona_matcher = PersonaMatcher()
        self.cache = QueryCache()
    
    @lru_cache(maxsize=1000)
    def quick_process(self, user_input: str, selected_persona: dict) -> dict:
        """Fluxo rápido usando persona pré-selecionada"""
        start = time.time()
        
        # 1. Construção Rápida de Query
        query = self.build_basic_query(user_input, selected_persona)
        
        # 2. Execução Imediata
        results = AthenaConnector().execute(query)
        
        return {
            "query": query,
            "results": results,
            "persona": selected_persona,
            "processing_time": time.time() - start
        }
    
    def suggest_better_persona(self, user_input: str, current_persona: dict) -> Optional[dict]:
        """Verificação em background de persona mais adequada"""
        start = time.time()
        
        # 1. Match Rápido com Cache
        suggested = self.persona_matcher.quick_match(user_input)
        
        # 2. Só retorna se for diferente e relevante
        if suggested["id"] != current_persona["id"] and suggested["confidence"] > 0.85:
            return {
                "suggestion": suggested,
                "analysis_time": time.time() - start
            }
        return None

class PersonaMatcher:
    @lru_cache(maxsize=1000)
    def quick_match(self, text: str) -> dict:
        """Versão leve do matcher usando keywords"""
        keywords = extract_keywords(text)
        
        # Procura direta em cache
        for persona in self.personas:
            if any(kw in persona["keywords"] for kw in keywords):
                return persona
        return self.default_persona

class QueryCache:
    def __init__(self):
        self.cache = {}
        self.hits = 0
    
    def get(self, query_signature: str) -> Optional[dict]:
        return self.cache.get(query_signature)
    
    def set(self, query_signature: str, result: dict):
        if len(self.cache) > 1000:  # Limite máximo
            self.cache.popitem()
        self.cache[query_signature] = result