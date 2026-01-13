from __future__ import annotations
import os
import socket
import threading
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import pandas as pd
from dotenv import load_dotenv
import mysql.connector

from pathlib import Path
import sys

LAST_ENV_PATH = None  # para debug

# =========================
# Paleta e Temas (TTK)
# =========================
LIGHT_PALETTE = {
    "bg": "#F7F8FA",
    "fg": "#1F2937",
    "subtle": "#6B7280",
    "card": "#FFFFFF",
    "border": "#E5E7EB",
    "primary": "#2563EB",
    "primary_fg": "#FFFFFF",
    "accent": "#7C3AED",
    "success": "#16A34A",
    "warning": "#D97706",
    "danger": "#DC2626",
    "row_even": "#F3F4F6",
    "row_odd": "#FFFFFF",
    "selection": "#DBEAFE",
    "header_bg": "#EEF2FF",
    "header_fg": "#111827",
}

def apply_style(root: tk.Tk, theme: str):
    """Aplica estilos ttk conforme tema ('light'/'dark')."""
    style = ttk.Style(root)
    # 'clam' é o mais consistente entre plataformas para customização
    try:
        style.theme_use('clam')
    except tk.TclError:
        pass

    pal = LIGHT_PALETTE

    # Janela & base
    root.configure(bg=pal["bg"])
    style.configure(".", background=pal["bg"], foreground=pal["fg"], fieldbackground=pal["card"])

    # Frames e Labels
    style.configure("Card.TFrame", background=pal["card"], bordercolor=pal["border"], relief="flat")
    style.configure("TLabel", background=pal["bg"], foreground=pal["fg"])
    style.configure("Subtle.TLabel", foreground=pal["subtle"])
    style.configure("Header.TLabel", font=("Segoe UI", 12, "bold"))

    # Entries / Combobox
    style.configure("TEntry", fieldbackground=pal["card"], foreground=pal["fg"], bordercolor=pal["border"])
    style.map("TEntry", bordercolor=[("focus", pal["primary"])])
    style.configure("TCombobox", fieldbackground=pal["card"], background=pal["card"], foreground=pal["fg"])
    style.map("TCombobox",
              fieldbackground=[("readonly", pal["card"])],
              selectbackground=[("readonly", pal["selection"])],
              selectforeground=[("readonly", pal["fg"])]
    )

    # Botões
    style.configure("Primary.TButton",
                    background=pal["primary"], foreground=pal["primary_fg"],
                    padding=8, focusthickness=3, focuscolor=pal["selection"])
    style.map("Primary.TButton",
              background=[("active", pal["accent"])],
              relief=[("pressed", "sunken")])

    style.configure("Ghost.TButton",
                    background=pal["card"], foreground=pal["fg"],
                    padding=8, bordercolor=pal["border"])
    style.map("Ghost.TButton",
              background=[("active", pal["row_even"])],
              relief=[("pressed", "sunken")])

    # Progressbar
    style.configure("Thin.Horizontal.TProgressbar", thickness=6, background=pal["primary"])

    # Treeview
    style.configure("Custom.Treeview",
                    background=pal["card"],
                    fieldbackground=pal["card"],
                    foreground=pal["fg"],
                    bordercolor=pal["border"],
                    rowheight=26)
    style.map("Custom.Treeview",
              background=[("selected", pal["selection"])],
              foreground=[("selected", pal["header_fg"])])

    style.configure("Custom.Treeview.Heading",
                    background=pal["header_bg"],
                    foreground=pal["header_fg"],
                    font=("Segoe UI", 10, "semibold"))
    style.map("Custom.Treeview.Heading",
              background=[("active", pal["header_bg"])])

    # Status bar label
    style.configure("Status.TLabel", background=pal["card"], foreground=pal["subtle"])

    # Separators / Borders
    style.configure("Line.TFrame", background=pal["border"])

    # Retorna a paleta para ser usada no código
    return pal

# =========================
# Config & Banco
# =========================
def carregar_variaveis_ambiente():
    global LAST_ENV_PATH
    load_dotenv(override=False)

    paths: List[Path] = []
    try:
        if getattr(sys, 'frozen', False):
            base_dir = Path(sys.executable).parent
            meipass = Path(getattr(sys, '_MEIPASS', base_dir))
            paths += [base_dir / '.env', meipass / '.env']
        else:
            base_dir = Path(__file__).parent
            paths += [base_dir / '.env']
    except Exception:
        pass

    paths.append(Path.cwd() / '.env')

    for p in paths:
        if p.exists():
            load_dotenv(dotenv_path=p, override=True)
            LAST_ENV_PATH = str(p)
            break

def obter_config_banco() -> dict:
    carregar_variaveis_ambiente()
    cfg = {
        'host': os.getenv('DB_HOST', ''),
        'user': os.getenv('DB_USER', ''),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_DATABASE', ''),
        'port': int(os.getenv('DB_PORT', '3306')),
        'timeout': int(os.getenv('DB_TIMEOUT', '15')),
    }

    auth_plugin = os.getenv('DB_AUTH_PLUGIN', '').strip()
    if auth_plugin:
        cfg['auth_plugin'] = auth_plugin
    ssl_ca = os.getenv('DB_SSL_CA', '').strip()
    ssl_cert = os.getenv('DB_SSL_CERT', '').strip()
    ssl_key = os.getenv('DB_SSL_KEY', '').strip()
    if ssl_ca:
        cfg['ssl_ca'] = ssl_ca
    if ssl_cert:
        cfg['ssl_cert'] = ssl_cert
    if ssl_key:
        cfg['ssl_key'] = ssl_key
    return cfg

colunas_encerramento = [
    'cnj',
    'valor_causa', 'valor_final_causa',
    'data_fase', 'fase',
    'data_status', 'status',
    'data_resultado', 'tipo_resultado',
    'parecer_processo',
    'verificado_encerramento',
    'encerramento_exportado',
    'cod_lote', 'cod_usuario_exportador',
    'carteira', 'cliente',
    'cod_status', 'cod_fase',
    'justificativa',
    'cod_usuario_encerrador',
    'cod_usuario_envio',
    'data_exportacao',
    'dataEnvio',
    'data_submit',
    'dataAtualizacao', 'codUsuarioAtualizacao',
    'codMotivo', 'motivo',
    'encerrado', 'exportado',
]

RENAME_MAP = {
    'Nº do Processo CNJ': 'cnj',
    'Cliente': 'cliente',
    'Valor da Causa': 'valor_causa',
    'Valor Final da Causa': 'valor_final_causa',
    'Data da Fase': 'data_fase',
    'Fase': 'fase',
    'Data do Status': 'data_status',
    'Status': 'status',
    'Data do Resultado': 'data_resultado',
    'Tipo de Resultado': 'tipo_resultado',
    'Parecer do Processo': 'parecer_processo',
}

TODAY_STR = datetime.now().strftime('%d/%m/%Y')
COMPANY_PRESETS: Dict[str, Dict[str, object]] = {
    'STONE MIDDLE': {
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'STONE MIDDLE {TODAY_STR}',
        'carteira': '58',
    },
    'STONE PASSIVO': {
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'STONE PASSIVO {TODAY_STR}',
        'carteira': '49',
    },
    'AMBEV CIVEL':{
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'AMBEV CIVEL {TODAY_STR}',
        'carteira': '1',
    },
    'AMBEV TRABALHISTA':{
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'AMBEV TRABALHISTA {TODAY_STR}',
        'carteira': '36',
    },
    'ANCAR':{
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'ANCAR {TODAY_STR}',
        'carteira': '3',
    },
    'ATIVOS':{
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'ATIVOS {TODAY_STR}',
        'carteira': '44',
    },
    'BRE - TRAB GERAL':{
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'BRE - TRAB GERAL {TODAY_STR}',
        'carteira': '33',
    },
    'BRE - CIVEL GERAL':{
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'BRE - CIVEL GERAL {TODAY_STR}',
        'carteira': '33',
    },

    'CB - TRAB':{
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'CB - TRAB {TODAY_STR}',
        'carteira': '33',
    },

    'CB - CIVEL':{
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'CB - CIVEL {TODAY_STR}',
        'carteira': '33',
    },

    'IMC - TRAB':{
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'IMC - TRAB {TODAY_STR}',
        'carteira': '33',
    },

    'IMC - CIVEL':{
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'IMC - CIVEL {TODAY_STR}',
        'carteira': '33',
    },

    'CAGECE':{
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'CAGECE {TODAY_STR}',
        'carteira': '6',
    },


    'CIVEL':{
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'CIVEL {TODAY_STR}',
            'carteira': '42',
    },

    'CIVEL -  CACAU SHOW':{
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'CIVEL -  CACAU SHOW {TODAY_STR}',
            'carteira': '42',
    },

    'COBRANÇA JUDICIAL': {
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'COBRANÇA JUDICIAL {TODAY_STR}',
        'carteira': '41',
    },

    'CONTRATOS': {
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'CONTRATOS  {TODAY_STR}',
        'carteira': '51',
    },

    'DIREITO MUNICIPAL': {
        'dataEnvio': TODAY_STR,
        'cod_usuario_envio': '222',
        'cod_lote': f'DIREITO MUNICIPAL  {TODAY_STR}',
        'carteira': '54',
    },

    'DIREITO PENAL': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'DIREITO PENAL  {TODAY_STR}',
            'carteira': '56',
        },

    'DIREITO TRIBUTARIO': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'DIREITO TRIBUTARIO  {TODAY_STR}',
            'carteira': '46',
    },

    'EDUCACIONAL - CIVEL': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'EDUCACIONAL - CIVEL  {TODAY_STR}',
            'carteira': '39',
    },

    'EDUCACIONAL - TRAB': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'EDUCACIONAL - TRAB  {TODAY_STR}',
            'carteira': '39',
    },

   'EGP': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'EGP  {TODAY_STR}',
            'carteira': '108',
    },

    'ENEL RJ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'ENEL RJ  {TODAY_STR}',
            'carteira': '101',
    },

    'ESTRATEGICOS': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'ESTRATEGICOS  {TODAY_STR}',
            'carteira': '45',
    },

    'IMOBILIARIO - CONTENCIOSO ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'IMOBILIARIO CONTENCIOSO {TODAY_STR}',
            'carteira': '43',
    },

    'IMOBILIARIO - TLSA ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'IMOBILIARIO TLSA {TODAY_STR}',
            'carteira': '43',
    },


'ISGH - CIVEL': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'ISGH -CIVEL{TODAY_STR}',
            'carteira': '38',
    },

'ISGH - TRABALHISTA': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'ISGH - TRABALHISTA {TODAY_STR}',
            'carteira': '47',
    },

'LICITAÇÕES': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'LICITAÇÕES {TODAY_STR}',
            'carteira': '53',
    },

'MOVIDA': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'MOVIDA {TODAY_STR}',
            'carteira': '20',
    },

'NOTREDAME - CIVEL': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'NOTREDAME CIVEL {TODAY_STR}',
            'carteira': '21',
    },
'NOTREDAME - ESTRATEGICO': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'NOTREDAME ESTRATEGICO{TODAY_STR}',
            'carteira': '52',
    },
'NOTREDAME - TRABALHISTA ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'NOTREDAME TRABALHISTA{TODAY_STR}',
            'carteira': '48',
    },
'DEXCO':{
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'DEXCO {TODAY_STR}',
            'carteira': '48',
},

'ORIGINAL ATIVO':{
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'ORIGINAL ATIVO {TODAY_STR}',
            'carteira': '61',
},

'ORIGINAL PASSIVO ':{
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'ORIGINAL PASSIVO {TODAY_STR}',
            'carteira': '60',
},

'PAGUE MENOS - CIVEL ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'PAGUE MENOS CIVEL{TODAY_STR}',
            'carteira': '29',
    },

'PAGUE MENOS - TRABALHISTA ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'PAGUE MENOS - TRABALHISTA{TODAY_STR}',
            'carteira': '35',
    },

'PICPAY PASSIVO  ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'PICPAY PASSIVO{TODAY_STR}',
            'carteira': '63',
    },

'PICPAY ATIVO  ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'PICPAY ATIVO{TODAY_STR}',
            'carteira': '66',
    },

'PORTO SEGURO ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'PORTO SEGURO{TODAY_STR}',
            'carteira': '27',
    },

'PUBLICO ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'PUBLICO{TODAY_STR}',
            'carteira': '55',
    },

'RD ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'RD{TODAY_STR}',
            'carteira': '59',
    },

'SERVIÇOS ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'SERVIÇOS{TODAY_STR}',
            'carteira': '32',
    },

'SOLAR BR - CIVEL': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'SOLAR BR - CIVEL{TODAY_STR}',
            'carteira': '32',
    },

'SOLAR BR - TRABALHISTA ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'SOLAR BR - TRABALHISTA {TODAY_STR}',
            'carteira': '37',
    },

'TLSA - CIVEL ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'TLSA - CIVEL {TODAY_STR}',
            'carteira': '28',
    },

'TLSA - TRAB ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'TLSA - TRAB {TODAY_STR}',
            'carteira': '34',
    },

'TRABALHISTA - GERAL': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'TRABALHISTA - GERAL {TODAY_STR}',
            'carteira': '40',
    },


'VALE CIVEL': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'VALE CIVEL {TODAY_STR}',
            'carteira': '30',
    },

'VERZANI': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'VERZANI {TODAY_STR}',
            'carteira': '57',
    },

'RJ E FALENCIA ': {
            'dataEnvio': TODAY_STR,
            'cod_usuario_envio': '222',
            'cod_lote': f'RJ E FALENCIA  {TODAY_STR}',
            'carteira': '64',
    },

}
COMMON_DEFAULTS = {
    'verificado_encerramento': 0,
    'encerramento_exportado': 0,
    'encerrado': None,
    'exportado': 0,
}

def aplicar_presets(df: pd.DataFrame, empresa: str) -> pd.DataFrame:
    df = df.copy()
    preset = COMPANY_PRESETS.get(empresa, {})
    for k_def, v_def in COMMON_DEFAULTS.items():
        if k_def not in df.columns:
            df[k_def] = v_def

    for col in ['cod_lote', 'cod_usuario_envio', 'carteira']:
        val = preset.get(col)
        if val is not None:
            df[col] = val

    data_envio = preset.get('dataEnvio')
    if data_envio:
        try:
            dt = pd.to_datetime(data_envio, dayfirst=True, errors='coerce')
            df['data_exportacao'] = dt.dt.date if isinstance(dt, pd.Series) else dt.date()
        except Exception:
            df['data_exportacao'] = date.today()
    else:
        if 'data_exportacao' not in df.columns:
            df['data_exportacao'] = None
    return df

def teste_tcp(host: str, port: int, timeout: float = 3.0) -> Tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, ""
    except Exception as e:
        return False, str(e)

def conectar_ao_mysql() -> Tuple[Optional[object], Optional[object]]:
    cfg = obter_config_banco()
    ok, err = teste_tcp(cfg['host'], cfg['port'], timeout=min(cfg.get('timeout', 15), 5))
    if not ok:
        messagebox.showerror(
            'Rede/Porta fechada',
            f"Não foi possível abrir TCP para {cfg['host']}:{cfg['port']}\n"
            f"Motivo: {err}\n\nVerifique firewall/NAT/segurança do servidor e se o MySQL está escutando."
        )
        return None, None

    def base_kwargs_connector():
        kw = dict(
            host=cfg['host'],
            user=cfg['user'],
            password=cfg['password'],
            database=cfg['database'],
            port=cfg['port'],
            connection_timeout=cfg.get('timeout', 15),
            use_pure=True,
        )
        for k in ('ssl_ca', 'ssl_cert', 'ssl_key'):
            if k in cfg:
                kw[k] = cfg[k]
        return kw

    try:
        conn = mysql.connector.connect(**base_kwargs_connector())
        return conn, conn.cursor()
    except mysql.connector.Error as err1:
        msg1 = str(err1).lower()
        if 'authentication plugin' in msg1 and 'mysql_native_password' in msg1 and 'not supported' in msg1:
            try:
                import pymysql
            except ImportError:
                messagebox.showerror(
                    'Dependência ausente',
                    'PyMySQL não está instalado.\n\nRode:\n'
                    '"C:\\Users\\Renan Farias\\AppData\\Local\\Programs\\Python\\Python313\\python.exe" -m pip install PyMySQL'
                )
                return None, None
            try:
                ssl_params = None
                if 'ssl_ca' in cfg or 'ssl_cert' in cfg or 'ssl_key' in cfg:
                    ssl_params = {}
                    if 'ssl_ca' in cfg:   ssl_params['ca']   = cfg['ssl_ca']
                    if 'ssl_cert' in cfg: ssl_params['cert'] = cfg['ssl_cert']
                    if 'ssl_key' in cfg:  ssl_params['key']  = cfg['ssl_key']

                conn = pymysql.connect(
                    host=cfg['host'],
                    user=cfg['user'],
                    password=cfg['password'],
                    database=cfg['database'],
                    port=cfg['port'],
                    connect_timeout=cfg.get('timeout', 15),
                    ssl=ssl_params
                )
                return conn, conn.cursor()
            except Exception as err2:
                messagebox.showerror(
                    'Erro PyMySQL',
                    f'Falha no fallback PyMySQL:\n{err2}\n\n'
                    f'Host={cfg.get("host")}\nDB={cfg.get("database")}\nPort={cfg.get("port")}\n'
                    f'.env usado: {LAST_ENV_PATH or "NÃO ENCONTRADO"}'
                )
                return None, None
        messagebox.showerror(
            'Erro MySQL',
            f"{err1}\n\nHost={cfg.get('host')}\nDB={cfg.get('database')}\nPort={cfg.get('port')}\n"
            f".env usado: {LAST_ENV_PATH or 'NÃO ENCONTRADO'}"
        )
        return None, None
    except Exception as e:
        messagebox.showerror('Erro inesperado', str(e))
        return None, None

def _parse_data_series(s: pd.Series) -> pd.Series:
    s = pd.to_datetime(s, errors='coerce', dayfirst=True)
    return s.dt.date

def formatar_datas_e_numeros(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for campo in ['valor_causa', 'valor_final_causa']:
        if campo in df.columns:
            df[campo] = pd.to_numeric(df[campo], errors='coerce').fillna(0.0)
    for c in list(df.columns):
        if isinstance(c, str) and c.startswith('data_'):
            df[c] = _parse_data_series(df[c])
    if 'dataAtualizacao' in df.columns:
        df['dataAtualizacao'] = _parse_data_series(df['dataAtualizacao'])
    if 'data_submit' in df.columns:
        df['data_submit'] = _parse_data_series(df['data_submit'])
    if 'cnj' in df.columns:
        df['cnj'] = df['cnj'].astype(str).str.strip()
    for c in ['fase', 'status', 'tipo_resultado', 'parecer_processo', 'cliente', 'justificativa', 'motivo', 'cod_lote']:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

def validar_colunas_para_insercao(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    faltando = [c for c in colunas_encerramento if c not in df.columns]
    return (len(faltando) == 0), faltando

def montar_registros(df: pd.DataFrame) -> List[Tuple]:
    registros: List[Tuple] = []
    for _, row in df.iterrows():
        vals: List[object] = []
        for coluna in colunas_encerramento:
            v = row.get(coluna, None)
            if pd.isna(v):
                vals.append(None)
            else:
                vals.append(v)
        registros.append(tuple(vals))
    return registros

def verificar_cnjs_existentes(cnjs: List[str]) -> List[str]:
    """Verifica quais CNJs já existem no banco."""
    conn, cur = conectar_ao_mysql()
    if not conn:
        return []
    try:
        if not cnjs:
            return []
        placeholders = ", ".join(["%s"] * len(cnjs))
        sql = f"SELECT DISTINCT cnj FROM encerramento WHERE cnj IN ({placeholders})"
        cur.execute(sql, cnjs)
        existentes = [row[0] for row in cur.fetchall()]
        return existentes
    except Exception as e:
        messagebox.showerror('Erro', f'Falha ao verificar CNJs existentes:\n{e}')
        return []
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass


def inserir_em_lotes(registros: List[Tuple], lote: int = 500, progress_cb=None) -> Tuple[int, List[str]]:
    """Retorna (total_inserido, lista_cnjs_duplicados)."""
    conn, cur = conectar_ao_mysql()
    if not conn:
        return 0, []

    # Extrair CNJs (primeiro campo de cada tupla)
    cnjs_todos = [reg[0] for reg in registros if reg[0]]
    cnjs_duplicados = verificar_cnjs_existentes(cnjs_todos)
    cnjs_duplicados_set = set(cnjs_duplicados)

    # Filtrar registros que não estão duplicados
    registros_validos = [reg for reg in registros if reg[0] not in cnjs_duplicados_set]

    if not registros_validos:
        return 0, cnjs_duplicados

    try:
        cols = ", ".join(colunas_encerramento)
        ph = ", ".join(["%s"] * len(colunas_encerramento))
        sql = f"INSERT INTO encerramento ({cols}) VALUES ({ph})"
        total = 0
        for i in range(0, len(registros_validos), lote):
            chunk = registros_validos[i:i + lote]
            cur.executemany(sql, chunk)
            conn.commit()
            total += cur.rowcount or 0
            if progress_cb:
                progress_cb(min(total, len(registros_validos)), len(registros))
        return total, cnjs_duplicados
    except mysql.connector.Error as err:
        try:
            conn.rollback()
        except Exception:
            pass
        messagebox.showerror('Erro MySQL', f'Falha ao inserir registros:\n{err}')
        return 0, cnjs_duplicados
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

# =========================
# UI Tkinter
# =========================
@dataclass()
class AppState:
    path: Optional[str] = None
    df: Optional[pd.DataFrame] = None
    empresa: str = ''
    sheet_name: Optional[str] = None

class MigracoesApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('Importador de Encerramentos')
        self.geometry('1200x720')

        # Tema atual
        self.theme = tk.StringVar(value="dark")  # "light" ou "dark"
        self.pal = apply_style(self, self.theme.get())

        self.state = AppState()
        self.create_widgets()
        self.bind_theme_switch()

    def bind_theme_switch(self):
        def on_change(*_):
            self.pal = apply_style(self, self.theme.get())
            # re-pintar áreas de cartão/linhas
            for w in (self.top_card, self.status_bar):
                w.configure(style="Card.TFrame")
        self.theme.trace_add("write", on_change)

    # -------- UI Builders --------
    def section_title(self, parent, text):
        lbl = ttk.Label(parent, text=text, style="Header.TLabel")
        lbl.pack(anchor="w", pady=(4, 6))
        ttk.Frame(parent, height=1, style="Line.TFrame").pack(fill="x", pady=(0, 10))

    def create_widgets(self):
        # Top card (fundo cartão)
        self.top_card = ttk.Frame(self, padding=16, style="Card.TFrame")
        self.top_card.pack(fill='x', padx=14, pady=14)

        # Título + tema
        top_header = ttk.Frame(self.top_card, style="Card.TFrame")
        top_header.pack(fill="x")
        ttk.Label(top_header, text="📤 Importador de Encerramentos", style="Header.TLabel").pack(side="left")
        theme_switch = ttk.Checkbutton(
            top_header,
            text="Tema escuro",
            variable=self.theme,
            onvalue="dark",
            offvalue="light",
            style="Ghost.TButton"
        )
        theme_switch.pack(side="right")

        # LINHA 1: arquivo
        row1 = ttk.Frame(self.top_card, style="Card.TFrame")
        row1.pack(fill='x', pady=(10, 6))
        ttk.Label(row1, text='Planilha:', style="TLabel").grid(row=0, column=0, sticky='w')
        self.ent_path = ttk.Entry(row1, width=90)
        self.ent_path.grid(row=0, column=1, padx=6, sticky="we")
        ttk.Button(row1, text='Selecionar...', style="Ghost.TButton", command=self.on_select_file)\
            .grid(row=0, column=2, padx=4)
        row1.columnconfigure(1, weight=1)

        # LINHA 2: sheet + empresa
        row2 = ttk.Frame(self.top_card, style="Card.TFrame")
        row2.pack(fill='x', pady=(4, 10))
        ttk.Label(row2, text='Aba (sheet):').grid(row=0, column=0, sticky='w', pady=(4, 0))
        self.cmb_sheet = ttk.Combobox(row2, values=[], state='readonly', width=30)
        self.cmb_sheet.grid(row=0, column=1, sticky='w', padx=(6, 20), pady=(4, 0))

        ttk.Label(row2, text='Empresa:').grid(row=0, column=2, sticky='e', pady=(4, 0))
        self.cmb_empresa = ttk.Combobox(row2, values=list(COMPANY_PRESETS.keys()), state='readonly', width=30)
        self.cmb_empresa.grid(row=0, column=3, sticky='w', padx=6, pady=(4, 0))

        # AÇÕES
        actions = ttk.Frame(self.top_card, style="Card.TFrame")
        actions.pack(fill='x', pady=(6, 0))
        ttk.Button(actions, text='Testar Conexão', style="Ghost.TButton", command=self.on_test_conn)\
            .pack(side='left')
        ttk.Button(actions, text='Pré-visualizar', style="Ghost.TButton", command=self.on_preview)\
            .pack(side='left', padx=8)
        ttk.Button(actions, text='Enviar ao Banco', style="Primary.TButton", command=self.on_send)\
            .pack(side='left')

        # LISTA (Treeview)
        self.tree = ttk.Treeview(self, style="Custom.Treeview", show='headings')
        self.tree.pack(fill='both', expand=True, padx=14, pady=(0, 10))

        # Barra de status
        self.status_bar = ttk.Frame(self, padding=(14, 8), style="Card.TFrame")
        self.status_bar.pack(fill='x', padx=14, pady=(0, 14))
        self.lbl_status = ttk.Label(self.status_bar, text='🟢 Pronto.', style='Status.TLabel')
        self.lbl_status.pack(side='left')
        self.pb = ttk.Progressbar(self.status_bar, mode='determinate', length=320, style="Thin.Horizontal.TProgressbar")
        self.pb.pack(side='right')

    # ---------- Handlers ----------
    def on_select_file(self):
        path = filedialog.askopenfilename(
            title='Selecione a planilha Excel',
            filetypes=[('Arquivos Excel', '*.xlsx *.xls')]
        )
        if not path:
            return
        self.state.path = path
        self.ent_path.delete(0, tk.END)
        self.ent_path.insert(0, path)
        try:
            xl = pd.ExcelFile(path)
            sheets = xl.sheet_names
            self.cmb_sheet['values'] = sheets
            if sheets:
                self.cmb_sheet.set(sheets[0])
                self.state.sheet_name = sheets[0]
        except Exception as e:
            messagebox.showerror('Erro', f'Não foi possível ler as abas:\n{e}')

    def on_test_conn(self):
        cfg = obter_config_banco()
        messagebox.showinfo(
            "Conexão",
            f"Host={cfg['host']}\nUser={cfg['user']}\nDB={cfg['database']}\n"
            f"Timeout={cfg.get('timeout', 15)}s\n.env: {LAST_ENV_PATH or 'NÃO ENCONTRADO'}"
        )
        self.set_status('🟦 Testando conexão...')
        def worker():
            conn, cur = conectar_ao_mysql()
            self.after(0, lambda: self._finish_test_conn(conn, cur))
        threading.Thread(target=worker, daemon=True).start()

    def _finish_test_conn(self, conn, cur):
        if conn:
            try:
                cur.execute('SELECT 1')
                messagebox.showinfo('Conexão', 'Conexão com MySQL OK!')
            except Exception as e:
                messagebox.showerror('Erro', f'Falha ao executar consulta de teste:\n{e}')
            finally:
                try:
                    cur.close(); conn.close()
                except Exception:
                    pass
            self.set_status('🟢 Pronto.')
        else:
            self.set_status('🔴 Falha na conexão.')

    def on_preview(self):
        if not self.state.path:
            messagebox.showwarning('Atenção', 'Selecione uma planilha primeiro.')
            return
        sheet = self.cmb_sheet.get() or 0
        empresa = self.cmb_empresa.get()
        if not empresa:
            messagebox.showwarning('Atenção', 'Selecione a empresa para aplicar os presets.')
            return
        try:
            self.set_status('🟦 Lendo planilha...')
            df = pd.read_excel(self.state.path, sheet_name=sheet)
            df = df.rename(columns=RENAME_MAP)
            df = aplicar_presets(df, empresa)
            df = formatar_datas_e_numeros(df)

            for col in colunas_encerramento:
                if col not in df.columns:
                    df[col] = None

            ok, faltando = validar_colunas_para_insercao(df)
            if not ok and faltando:
                messagebox.showwarning(
                    'Colunas ausentes',
                    f'Estas colunas irão como NULL: {faltando[:20]}'
                    + ('...' if len(faltando) > 20 else '')
                )

            self.state.df = df
            self.state.empresa = empresa
            self._render_preview(df)
            self.set_status(f'🟢 Pré-visualização OK – {len(df)} linhas.')
        except KeyError as ke:
            messagebox.showerror('Erro de coluna', f'Coluna ausente na planilha: {ke}')
            self.set_status('🔴 Erro na pré-visualização.')
        except Exception as e:
            messagebox.showerror('Erro', f'Falha ao pré-visualizar:\n{e}')
            self.set_status('🔴 Erro na pré-visualização.')

    def _render_preview(self, df: pd.DataFrame, max_rows: int = 300):
        # Reset
        for col in self.tree['columns']:
            self.tree.heading(col, text='')
        self.tree.delete(*self.tree.get_children())

        show_df = df.copy().head(max_rows)
        cols = list(show_df.columns)
        self.tree['columns'] = cols
        # Cabeçalhos
        for c in cols:
            self.tree.heading(c, text=c, anchor='w')
            self.tree.column(c, width=150, stretch=True, anchor='w')

        # Listras (tags)
        self.tree.tag_configure('oddrow', background=self.pal["row_odd"])
        self.tree.tag_configure('evenrow', background=self.pal["row_even"])

        for idx, (_, row) in enumerate(show_df.iterrows()):
            values = [("" if pd.isna(row[c]) else str(row[c])) for c in cols]
            tag = 'evenrow' if idx % 2 == 0 else 'oddrow'
            self.tree.insert('', 'end', values=values, tags=(tag,))

    def on_send(self):
        if self.state.df is None or self.state.df.empty:
            messagebox.showwarning('Atenção', 'Faça a pré-visualização antes de enviar.')
            return

        missing = [c for c in colunas_encerramento if c not in self.state.df.columns]
        if missing:
            if not messagebox.askyesno(
                'Colunas faltando',
                f'Estas colunas irão como NULL: {missing[:20]}'
                + ('...' if len(missing) > 20 else '') +
                '\nDeseja continuar?'
            ):
                return

        df = self.state.df.copy()
        registros = montar_registros(df)

        self.pb['value'] = 0
        self.pb['maximum'] = len(registros)
        self.set_status('🟦 Enviando ao banco...')

        def progress_cb(done, total):
            self.pb['value'] = done
            self.set_status(f'🟦 Inserindo... {done}/{total}')

        def worker():
            total = inserir_em_lotes(registros, lote=500, progress_cb=progress_cb)
            self.after(0, lambda: (
                self.set_status(f'🟢 Concluído. Inseridos {total} registros.'),
                messagebox.showinfo('Finalizado', f'Inseridos {total} registros.')
            ))
        threading.Thread(target=worker, daemon=True).start()

    # Util
    def set_status(self, text: str):
        self.lbl_status.configure(text=text)

# ==============================
# Main
# ==============================
if __name__ == '__main__':
    app = MigracoesApp()
    app.mainloop()
