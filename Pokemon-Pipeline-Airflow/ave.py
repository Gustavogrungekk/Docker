import os

# Definição da estrutura do projeto: diretórios e arquivos
project_structure = {
    "chatbot/app/core/query_processing": ["nlp_analyzer.py", "sql_generator.py"],
    "chatbot/app/core/data": ["connector.py", "cache.py"],
    "chatbot/app/streamlit/ui_components": ["sidebar.py", "chat_history.py"],
    "chatbot/app/streamlit/personas": ["persona_manager.py"],
    "chatbot/app/streamlit/dashboards": ["chart_renderer.py"],
    "chatbot/config": ["settings.yaml", "aws_config.yaml", "personas.json"],
    "chatbot/data/raw": [],
    "chatbot/data/processed": [],
    "chatbot/data/sql_templates": ["default_query.sql"],
    "chatbot/docs/user_guide": ["README.md"],
    "chatbot/docs/technical": ["architecture.md"],
    "chatbot/templates/assets/logos": [],
    "chatbot/templates/assets/styles": ["style.css"],
    "chatbot/utils/aws": ["s3_client.py", "athena_client.py"],
    "chatbot/utils/helpers": ["formatters.py", "validators.py"],
    "chatbot/utils/logging": ["logger_config.py"],
    "chatbot/tests/unit/core": [],
    "chatbot/tests/unit/utils": [],
    "chatbot/tests/integration": [],
    "chatbot/scripts/deployment": ["deploy_aws.sh", "ecs_config.yaml"],
    "chatbot/scripts/data_processing": ["data_pipeline.py"],
    "chatbot/models/nlp": [],
    "chatbot/models/ml": [],
}

# Criar diretórios e arquivos
for directory, files in project_structure.items():
    os.makedirs(directory, exist_ok=True)  # Criar diretório se não existir
    for file in files:
        file_path = os.path.join(directory, file)
        if not os.path.exists(file_path):  # Criar arquivo apenas se não existir
            with open(file_path, "w") as f:
                f.write("")  # Criar arquivo vazio

# Criar arquivos na raiz do projeto
root_files = [
    ".gitignore",
    "README.md",
    "requirements.txt",
    "Dockerfile",
    "Makefile",
    "setup.py"
]

for file in root_files:
    if not os.path.exists(file):  # Criar apenas se não existir
        with open(file, "w") as f:
            f.write("")

print("🚀 Estrutura do projeto criada com sucesso!")
