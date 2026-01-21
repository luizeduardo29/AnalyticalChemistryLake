 ![PPGI](ppgi-ufrj.png)

# AnalyticalChemistryLake

O **ChemistryAnalystLake** é um método para **converter, acessar e estruturar dados de análises químicas** a partir de arquivos brutos, armazenando-os em um **Data Lake analítico baseado em ClickHouse**.

O método suporta:

- **LCHR-MS (Thermo)** — arquivos `.raw`
- **GC-MS (Agilent – modo SIM)** — pastas `.D`

Todo o processamento é feito a partir de arquivos **mzML**, gerados com o **ProteoWizard (msconvert)**.

---

## Visão geral do método

O fluxo completo do método é:

1. Varredura recursiva de arquivos `.raw` e pastas `.D`
2. Conversão para `.mzML` usando `msconvert`
3. Leitura estruturada do mzML
4. Separação lógica por amostra e canal
5. Armazenamento em tabelas otimizadas no ClickHouse
6. Visualização dos dados (em desenvolvimento)

---

## Requisitos

Instale previamente no computador:

- Python 3.10  
- Docker  
- ProteoWizard  
  Versão recomendada: `3.0.25218.15b0739`  
  Outras versões funcionam, mas pode ser necessário ajustar o caminho do `msconvert.exe`.

---

## Instalação

### 1. Subir o ClickHouse via Docker

Na raiz do repositório, execute:

docker compose up -d

Verifique se o container está ativo:

docker ps

---

### 2. Criar o banco e as tabelas

Execute:

python create_tables.py

Esse script cria o database `analyticalChemistryLake`, todas as tabelas do método e os índices necessários.

---

### 3. Criar e ativar o ambiente virtual

Windows (PowerShell)

python -m venv venv  
.\venv\Scripts\Activate.ps1

Windows (CMD)

python -m venv venv  
venv\Scripts\activate

Linux / macOS

python3 -m venv venv  
source venv/bin/activate

---

### 4. Instalar dependências

pip install -r requirements.txt

---

## Processamento dos dados

O script `process_data.py` executa todo o pipeline de ingestão.

Ele identifica automaticamente o tipo de dado:

- `.raw` → LCHR-MS  
- `.D` → GC-MS SIM  

Converte os arquivos para mzML, lê os dados e insere cromatogramas e espectros no ClickHouse.

---

### Execução

python process_data.py  
--input "C:\Caminho\Para\Dados"  
--out "C:\Caminho\Para\mzml_tmp"

---

### Parâmetros disponíveis

- `--input` (obrigatório)  
  Diretório contendo arquivos `.raw` e/ou pastas `.D` (busca recursiva).

- `--out` (obrigatório)  
  Diretório onde os arquivos `.mzML` serão gerados temporariamente.

Exemplo completo:

python process_data.py  
--input "C:\Dados\Analises"  
--out "C:\mzml_out"  

---

## Modelo de dados (resumo)

O método utiliza as seguintes tabelas no ClickHouse:

### samples
Cadastro das amostras.

### sample_channels
Representa os canais de aquisição por amostra.  
Apenas um dos campos abaixo é preenchido:

- `scan_filter` → LCHR-MS (scans e espectros)
- `sim_ion_name` → GC-MS SIM ou cromatogramas LC-MS

### chromatogram_points
Pontos do cromatograma (`rt`, `intensity`).

### lcms_scans
Scans LC-MS (`scan_index`, `rt`, `ms_level`).

### lcms_spectra_points
Pontos do espectro LC-MS (`mz`, `intensity`).

---

## Visualização dos dados

Em desenvolvimento.

O script `view.py` será responsável por:

- Listar amostras e canais
- Visualizar cromatogramas (LCHR-MS e GC-MS SIM)
- Visualizar espectros LCHR-MS
- Gerar XIC por faixa de massa

Execução prevista:

python view.py

---

## Observações finais

- O caminho do `msconvert.exe` pode precisar ser ajustado no `process_data.py`
- Arquivos LCHR-MS normalmente contêm **spectra e cromatogramas**, ambos são ingeridos
- GC-MS SIM cria um canal por íon monitorado
- O método foi projetado para **alto volume de dados**, utilizando compressão e índices do ClickHouse
