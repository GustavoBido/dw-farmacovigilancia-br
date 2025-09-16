# Escopo  DW de Farmacovigilância (Anvisa)

**Objetivo:** montar um DW simples (bronze → silver → gold) no Azure + Databricks para analisar notificações de eventos adversos (VigiMed) e preparar base para cruzar com Registros e SNGPC depois.

**Perguntas principais:**
1) Quantas notificações por ano/mês e por UF?
2) Quais reações mais registradas por medicamento?
3) Percentual de casos graves por UF e por medicamento.
4) Evolução temporal de notificações por princípio ativo.

**Limites:** são suspeitas de eventos adversos (não prova de causalidade). Dados já são anonimizados. Usar amostras pequenas no início.

**Saída:** modelo estrela simples para Power BI.
