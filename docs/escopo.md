# Escopo  DW de Farmacovigilância (Anvisa)
Objetivo, perguntas de negócio e limites do projeto.
🚦 Etapa 2 — Preenchendo os docs

docs/escopo.md
Escreva em 5–10 linhas:

Objetivo: montar um DW em bronze/silver/gold para dados de farmacovigilância.

Perguntas que o BI deve responder (3–5, como quais reações são mais notificadas, distribuição por UF, evolução por ano).

Limites: é suspeita de evento adverso, não causalidade.

docs/fontes.md
Liste as fontes que você já achou:

VigiMed_Notificacoes.csv — atualizado em 12/09/2025.

VigiMed_Medicamentos.csv — idem.

VigiMed_Reacoes.csv — idem.

(futuro) Registros de Medicamentos.

(futuro) Vendas SNGPC.
Coloque URL oficial e data de coleta.

docs/modelo-logico.md
Rabisque o esquema estrela (pode ser texto ou até ASCII):

Fato: evento_medicamento (grão = notificação × medicamento).

Dims: medicamento, reação, tempo, local.

Bridge: evento_reacao (para representar N reações por evento).

docs/convencoes.md
Decida e anote:

Bronze = CSV cru + extract_date.

Silver = padronização de tipos, dicionário aplicado.

Gold = star schema pronto para Power BI.

Timezone = UTC.

Formato = Delta no silver/gold.

docs/decisoes.md
Exemplo:

[2025-09-16] Grão definido: fato_evento_medicamento.
[2025-09-16] Decidido usar Delta a partir do Silver.
