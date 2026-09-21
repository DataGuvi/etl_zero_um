import ast
with open('consume_api.py', encoding='utf-8') as f:
    arvore = ast.parse(f.read())
for node in ast.walk(arvore):
    if isinstance(node, ast.ClassDef) and node.name == 'ConsumeAPI':
        metodos = [n.name for n in node.body if isinstance(n, ast.FunctionDef)]
        for alvo in ['processa_bonus', 'processa_bonus_backfill', '_valida_bloco_bonus',
                     '_sql_fact_user_bonus', '_sql_dim_bonus', '_sql_bridge_bonus_product',
                     'extrai_fact_user_bonus_por_periodo', '_formata_duracao']:
            print(f'{alvo}: {"OK" if alvo in metodos else "FALTANDO — checar indentação"}')