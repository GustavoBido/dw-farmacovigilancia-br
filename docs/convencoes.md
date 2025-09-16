# Convenções Bronze / Silver / Gold

- **Bronze**: CSV cru, sem alterar, particionado por `extract_date=YYYY-MM-DD`.
- **Silver**: padronização de tipos, renomeação consistente, chaves técnicas.
- **Gold**: fato + dimensões prontos para Power BI.
- **Timezone**: UTC nas cargas.
- **Formato**: CSV no bronze; Delta no silver/gold.
- **Qualidade mínima**: contagem de linhas, colunas obrigatórias não nulas (id_notificacao, datas).
