create database servicelayer_betha_apitela_pmitatiaia
  with
  owner = postgres
  template = template0
  encoding = 'UTF8'
  lc_collate = 'Portuguese_Brazil.1252'
  lc_ctype = 'Portuguese_Brazil.1252'
  tablespace = pg_default
  connection limit = -1;


CREATE TABLE IF NOT EXISTS public.imoveis_dados
(
    id integer NOT NULL DEFAULT nextval('imoveis_dados_id_seq'::regclass),
    id_imovel integer NOT NULL,
    codigo_imovel character varying(100) COLLATE pg_catalog."default" NOT NULL,
    situacao text COLLATE pg_catalog."default",
    dados_json jsonb,
    CONSTRAINT imoveis_dados_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.imoveis_dados
    OWNER to postgres;



CREATE TABLE IF NOT EXISTS public.economicos_dados
(
    id integer NOT NULL DEFAULT nextval('economicos_dados_id_seq'::regclass),
    id_economico integer NOT NULL,
    codigo_economico character varying(100) COLLATE pg_catalog."default" NOT NULL,
    situacao text COLLATE pg_catalog."default",
    dados_json jsonb,
    CONSTRAINT economicos_dados_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.economicos_dados
    OWNER to postgres;



CREATE TABLE IF NOT EXISTS public.contribuintes_dados
(
    id integer NOT NULL DEFAULT nextval('contribuintes_dados_id_seq'::regclass),
    id_contribuinte integer NOT NULL,
    codigo_contribuinte character varying(100) COLLATE pg_catalog."default" NOT NULL,
    situacao text COLLATE pg_catalog."default",
    dados_json jsonb,
    CONSTRAINT contribuintes_dados_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.contribuintes_dados
    OWNER to postgres;   



CREATE TABLE IF NOT EXISTS public.planta_valores_dados
(
    id integer NOT NULL DEFAULT nextval('planta_valores_dados_id_seq'::regclass),
    id_planta integer NOT NULL,
    codigo_planta character varying(100) COLLATE pg_catalog."default" NOT NULL,
    situacao text COLLATE pg_catalog."default",
    dados_json jsonb,
    CONSTRAINT planta_valores_dados_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.planta_valores_dados
    OWNER to postgres;




CREATE TABLE IF NOT EXISTS public.contribuintes_filtro
(
    id_contribuinte integer NOT NULL,
    id_bairro integer NOT NULL
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.contribuintes_filtro
    OWNER to postgres;



-- Queries acompanhamento


select 
codigo_imovel as "Código Imóvel",
coalesce((substring(situacao from 'ERRO -> 422 (.*)')::jsonb->>'detail')::jsonb->>'message',
(substring(situacao from 'ERRO -> 422 (.*)')::jsonb->>'detail')::jsonb->>'ImovelProprietario')AS "Mensagem de erro",
coalesce((dados_json -> 'bairro' ->>'codigo'),'0') || ' - ' || (dados_json -> 'bairro' ->>'nome') AS "Bairro(Código-Nome)"
from imoveis_dados
where situacao <> 'SUCESSO'
order by "Bairro(Código-Nome)" asc, codigo_imovel::int asc

select 
codigo_economico as "Código Econômico",
coalesce((substring(situacao from 'ERRO -> 422 (.*)')::jsonb->>'detail')::jsonb->>'message',
(substring(situacao from 'ERRO -> 422 (.*)')::jsonb->>'detail')::jsonb->>'PessoaFisicaDocumento') AS "Mensagem de erro",
coalesce((dados_json -> 'bairro' ->>'codigo'),'0') || ' - ' || (dados_json -> 'bairro' ->>'nome') AS "Bairro(Código-Nome)"
from economicos_dados where situacao <> 'SUCESSO'
order by "Bairro(Código-Nome)" asc, codigo_economico::int asc


SELECT
codigo_contribuinte AS "Código Contribuinte",
COALESCE((substring(situacao from 'ERRO -> 422 (.*)')::jsonb->>'detail')::jsonb->>'message',
(substring(situacao from 'ERRO -> 422 (.*)')::jsonb->>'detail')::jsonb->>'PessoaFisicaDocumento') AS "Mensagem de erro",
COALESCE((dados_json -> 'enderecos' -> 0 -> 'bairro' ->> 'codigo'), '0'
) || ' - ' || (dados_json -> 'enderecos' -> 0 -> 'bairro' ->> 'nome') AS "Bairro(Código-Nome)"
FROM contribuintes_dados WHERE situacao <> 'SUCESSO'
AND (dados_json -> 'enderecos' -> 0 ->> 'principal') = 'SIM'
ORDER BY "Bairro(Código-Nome)" ASC, codigo_contribuinte::int ASC;

select 
codigo_planta as "Código Planta e Valores",
(substring(situacao from 'ERRO -> 400 (.*)')::jsonb->>'message') AS "Mensagem de erro",
coalesce((dados_json -> 'bairros' ->>'codigo'),'0') || ' - ' || (dados_json -> 'bairros' ->>'nome') AS "Bairro(Código-Nome)"
from planta_valores_dados where situacao <> 'SUCESSO'
order by "Bairro(Código-Nome)" asc, codigo_planta asc


select * from imoveis_dados --where situacao <> 'SUCESSO'
select * from economicos_dados --where situacao <> 'SUCESSO'
select * from contribuintes_dados --where situacao <> 'SUCESSO'
select * from planta_valores_dados --where situacao <> 'SUCESSO'
