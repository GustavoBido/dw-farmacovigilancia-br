# Data Warehouse de Farmacovigilância (Brasil - Anvisa)

Este projeto constrói um **Data Warehouse (DW)** para análise de **eventos adversos a medicamentos e vacinas** no Brasil, a partir dos dados abertos de Farmacovigilância da **Anvisa (VigiMed)**.

O objetivo é oferecer uma arquitetura **simples** e objetiva, utilizando serviços gratuitos do Azure + Databricks + Power BI, no modelo de **Lakehouse** com camadas Bronze, Silver e Gold.

---

## Arquitetura

ANVISA Dados Abertos (CSV)
│
▼
Azure Data Lake Storage Gen2 (ADLS)
├── bronze/ → dados crus (CSV originais)
├── silver/ → dados tratados e tipados
└── gold/ → modelo analítico (esquema estrela)
│
▼
Azure Databricks (ETL/Transformações)
│
▼
Power BI (dashboards interativos)


---

## Tecnologias

- **Azure**
  - Storage Account (ADLS Gen2)
  - Databricks (Community/Free Tier)
- **PySpark / Delta Lake**
- **Power BI** (desktop + service)
- **GitHub** (versionamento e docs)

---

##  Camadas do Lakehouse

### Bronze
- Dados crus da Anvisa (CSV, separador `;`, encoding ISO-8859-1).  
- Sem transformação, apenas ingestão no ADLS.

### Silver
- Dados limpos e tipados:
  - Conversão de datas (`yyyyMMdd`, `yyyyMM` → `date`).
  - Tipagem de colunas numéricas (idade, peso, altura).
  - Remoção de placeholders (`NULL`, `NaN`, `_x000D_`).
  - Normalização de textos.
- Tabelas no Silver:
  - `vigimed/notificacoes`
  - `vigimed/medicamentos`
  - `vigimed/reacoes`

### Gold
- Modelo em **esquema estrela**:
  - **Fato**: `fato_evento_medicamento` → (notificação × medicamento).
  - **Dimensões**:
    - `dim_tempo` (ano, mês, dia, ano_mes).
    - `dim_local` (UF, estados).
    - `dim_medicamento` (nome, princípio ativo, ATC, detentor).
    - `dim_reacao` (MedDRA: PT, HLT, HLGT, SOC).
  - **Ponte**:
    - `bridge_evento_reacao` (liga evento-medicamento às reações).

---

## Esquema Estrela

```mermaid
erDiagram
    dim_tempo ||--o{ fato_evento_medicamento : "id_tempo"
    dim_local ||--o{ fato_evento_medicamento : "id_local"
    dim_medicamento ||--o{ fato_evento_medicamento : "id_medicamento"
    dim_reacao ||--o{ bridge_evento_reacao : "id_reacao"
    fato_evento_medicamento ||--o{ bridge_evento_reacao : "identificacao_notificacao"
