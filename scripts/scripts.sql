create database servicelayer_betha_apitela_pmitatiaia
  with
  owner = postgres
  template = template0
  encoding = 'UTF8'
  lc_collate = 'Portuguese_Brazil.1252'
  lc_ctype = 'Portuguese_Brazil.1252'
  tablespace = pg_default
  connection limit = -1;


CREATE TABLE IF NOT EXISTS public.imoveis_logradouro_dados (
    id SERIAL PRIMARY KEY,
    id_imovel INTEGER NOT NULL,
    codigo_imovel VARCHAR(100),
    situacao TEXT,
    dados_json JSONB
);


CREATE TABLE IF NOT EXISTS public.economicos_logradouro_dados (
    id SERIAL PRIMARY KEY,
    id_economico INTEGER NOT NULL,
    codigo_economico VARCHAR(100) NOT NULL,
    situacao TEXT,
    dados_json JSONB
);


CREATE TABLE IF NOT EXISTS public.contribuintes_logradouro_dados (
    id SERIAL PRIMARY KEY,
    id_contribuinte INTEGER NOT NULL,
    codigo_contribuinte VARCHAR(100) NOT NULL,
    situacao TEXT,
    dados_json JSONB
); 



CREATE TABLE IF NOT EXISTS public.planta_valores_logradouro_dados (
    id SERIAL PRIMARY KEY,
    id_planta INTEGER NOT NULL,
    codigo_planta VARCHAR(100) NOT NULL,
    situacao TEXT,
    dados_json JSONB
);



CREATE TABLE IF NOT EXISTS public.contribuintes_logradouro_filtro
(
    id_contribuinte integer NOT NULL,
    id_logradouro integer NOT NULL
);



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
