# Comparativo: Conversão Automática (IA) vs Entrega Manual (Equipe)  
**Projeto: EXTRATO-TAS – PowerCenter → DBT**

## 📝 Visão Geral

Este repositório documenta as diferenças entre a conversão automática gerada por uma IA a partir de workflows PowerCenter e a entrega manual feita pela equipe de engenharia de dados utilizando DBT.

> ⚠️ O objetivo é destacar onde o trabalho humano foi **indispensável** — tanto na correção quanto na implementação de boas práticas e padrões corporativos.

---

## 🔍 Principais Diferenças

### 📁 Estrutura de Diretórios

| Aspecto                  | IA                            | Entregue                              | Observações |
|--------------------------|-------------------------------|----------------------------------------|-------------|
| Organização de pastas    | Tudo na raiz, sem estrutura   | `models/`, `macros/`, `sources/`, etc | IA não gera estrutura DBT válida |
| Tipos de arquivos        | `.sql`, `.yml` misturados     | Separação correta por função          | Essencial para CI/CD funcionar |

---

## 🧠 Comparativo de Código SQL

### IA (gerado automaticamente)
```sql
SELECT
    A.CD_CLIENTE,
    A.VL_SALDO
FROM
    TABELA_CLIENTE A
```

### Entregue (modelo DBT com boas práticas)
```sql
{{ config(
    materialized='incremental',
    unique_key='cd_cliente',
) }}

SELECT
    SAFE_CAST(A.CD_CLIENTE AS NUMERIC) AS cd_cliente,
    SAFE_CAST(A.VL_SALDO AS NUMERIC) AS vl_saldo,
    CURRENT_TIMESTAMP() AS dt_ingestion,
    1 AS is_active,
    0 AS is_deleted
FROM
    {{ ref('stg_tabela_cliente') }} A
WHERE
    A.dt_reference >= (SELECT MAX(dt_reference) FROM {{ this }})
```

| Aspecto              | IA         | Entregue     | Observações |
|----------------------|------------|--------------|-------------|
| Materialização       | Não existe | `incremental`| DBT exige para controle de execução |
| Filtro incremental   | Não existe | Implementado | Evita retrabalho e leitura desnecessária |
| Uso de `ref()`       | Não        | Sim          | Boas práticas DBT |

---

## 🧾 Comparativo YAML

| Aspecto                         | IA                  | Entregue              | Observações |
|---------------------------------|---------------------|------------------------|-------------|
| Descrição das colunas           | Inexistente         | Presentes e completas | Requisito de governança |
| Tipagem dos campos (`data_type`)| Ausente             | Definida corretamente | IA não gera |
| Organização do YAML             | Mínima / inválida   | Estruturada e validada | Necessário para `dbt docs` |

---

## ⚙️ Implementações Relevantes Feitas Manualmente

### 1. Uso de Parâmetros
- **IA:** Ignora variáveis e não converte nenhum parâmetro reutilizável.
- **Equipe:**
  - Criamos uma *worker* específica para ler e propagar parâmetros.
  - Implementamos `CROSS JOIN` com tabela de parâmetros.
  - Permitimos reutilização e modularidade entre os modelos.

### 2. Controle de Carga (Lógica Delta)
- **IA:** Sempre lê toda a base. Sem uso de datas ou logs.
- **Equipe:**
  - Leitura da última data da tabela log.
  - Cálculo automático de `data + 1` para próxima execução.
  - Eficiência e segurança na carga incremental.

---

## 📂 Repositório Comparativo

A análise linha-a-linha dos códigos `.sql` e `.yml` gerados pela IA versus os entregues manualmente está disponível [aqui](#) (link a ser atualizado com o repositório GitHub específico).

---

## ✅ Conclusão

Apesar de auxiliar em tarefas simples, a IA atualmente **não é capaz de gerar scripts DBT prontos para produção**. O esforço de engenharia ainda é crucial para:

- Estruturação correta do projeto;
- Governança de metadados;
- Implementação de lógica de carga;
- Aplicação de padrões de qualidade exigidos em ambientes corporativos.
