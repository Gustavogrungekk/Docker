import json
from typing import Dict, List

class QueryBuilder:
    def __init__(self, metadata_path: str):
        self.metadata = self._load_metadata(metadata_path)
    
    def _load_metadata(self, path: str) -> Dict:
        with open(path) as f:
            return json.load(f)
    
    def generate_query(self, question: str, persona: str) -> str:
        """Core query generation logic"""
        table = self._select_table(question)
        columns = self._select_columns(question, table)
        filters = self._build_filters(question)
        
        return f"""
            SELECT {columns}
            FROM {table['table_name']}
            {filters}
            {self._get_limit_clause(persona)}
        """
    
    def _select_table(self, question: str) -> Dict:
        # Simple table selection logic
        if "sale" in question.lower():
            return next(t for t in self.metadata['tables'] if t['table_name'] == 'sales_transactions')
        # Add other table matching logic
    
    def _select_columns(self, question: str, table: Dict) -> str:
        # Column selection based on question keywords
        if "total" in question:
            return "SUM(amount) AS total"
        if "average" in question:
            return "AVG(unit_price) AS average_price"
        return "*"
    
    def validate_query(self, query: str) -> bool:
        """Basic security validation"""
        forbidden_keywords = ["DELETE", "UPDATE", "INSERT"]
        return not any(kw in query.upper() for kw in forbidden_keywords)