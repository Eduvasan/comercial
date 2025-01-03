# Databricks notebook source
# MAGIC %md
# MAGIC ## CONSULTAR CONVERSACIONES

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select * from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # CUANTIFICACIONES DE ENROLADOS

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select
# MAGIC month,
# MAGIC count(distinct(registro))
# MAGIC FROM hive_metastore.avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and codigo = 'TRX_ENROLAMIENTO_PEND'
# MAGIC group by month
# MAGIC order by month

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select
# MAGIC month,
# MAGIC count(distinct(session))
# MAGIC FROM hive_metastore.avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and codigo = 'TRX_ENROLAMIENTO_PEND'
# MAGIC group by month
# MAGIC order by month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANUAL
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select
# MAGIC year,
# MAGIC count(distinct(session))
# MAGIC FROM hive_metastore.avireportscomercial.conversaciones_avi_comercial
# MAGIC --where codigo = 'TRX_ENROLAMIENTO_PEND'
# MAGIC group by year
# MAGIC order by year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANUAL
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select
# MAGIC distinct(year)
# MAGIC FROM hive_metastore.avireportscomercial.conversaciones_avi_comercial
# MAGIC --where codigo = 'TRX_ENROLAMIENTO_PEND'

# COMMAND ----------

# MAGIC %md
# MAGIC ## CUANTIFICACION CONSULTAS A LA OPCION DE SALDO, NUMERO DE CUENTA Y MOVIMIENTOS

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select
# MAGIC count(distinct(session)),
# MAGIC month
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and codigo = 'TRX_ACCOUNT_BALANCE_PEND'
# MAGIC group by month
# MAGIC order by month

# COMMAND ----------

# MAGIC %md
# MAGIC ## CONSULTA AL MENU SALDOS, NUMERO DE CUENTA Y MOVIMIENTOS

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select * from avireportscomercial.conversaciones_avi_comercial
# MAGIC where session in (select distinct(session)
# MAGIC from avireportscomercial.conversaciones_avi_comercial 
# MAGIC where year = 2024 and month = 9 and codigo like '%MORA%')

# COMMAND ----------

# MAGIC %md
# MAGIC ## CUANTIFICACIÓN DE SESIONES Y USUARIOS

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select count(distinct(registro)),
# MAGIC month
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and month in (1,2,3)
# MAGIC group by month
# MAGIC order by month
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select * from avireportscomercial.conversaciones_avi_comercial
# MAGIC where session in (select distinct(session)
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and codigo = 'IZIP_001') 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXTRACCION DE ENROLAMIENTO DE TERADATA (CONE_007 o TRX_ENROLAMIENTO_PEND)
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select
# MAGIC count(distinct(registro)),
# MAGIC month
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and codigo = 'TRX_ENROLAMIENTO_PEND'
# MAGIC group by month
# MAGIC order by month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GEN AI TOKEN
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select *
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where session in (year = 2024 and month in (1,2) and codigo = 'Pregunta' and mensaje like '%toquen%' or mensaje like '%TOQUEN%' or mensaje like '%token%' or mensaje like '%TOKEN%')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVISON HISOTORIA DE SALDOS - 09/02
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select COUNT(distinct(session))
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and month in (2,3) and codigo = 'TRX_ACCOUNT_BALANCE_PEND'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVISON HISOTORIA DE SALDOS - 09/02
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC SELECT * FROM (select distinct(session)
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and month in (2,3) and codigo = 'TRX_ACCOUNT_BALANCE_PEND') a
# MAGIC INNER JOIN (select distinct(session)
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and month in (2,3) and codigo = 'TRX_BALANCE_DETAILS') b on a.session=b.session

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVISON HISOTORIA DE SALDOS - 09/02
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC SELECT * FROM avireportscomercial.conversaciones_avi_comercial WHERE session='0b760186-672a-46d2-84ae-05aa7eebbde9'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVISON HISOTORIA DE SALDOS - 09/02
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select *
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where session in (select distinct(session)
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and month in (2,3) and codigo = 'TRX_ACCOUNT_BALANCE_PEND')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVISON HISOTORIA DE SALDOS - 09/02
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select *
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where session in (select distinct(session)
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and month in (2,3) and codigo = 'TRX_BALANCE_DETAILS')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GEN AI TOKEN
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select *
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and month = 2 and day = 20 and codigo = 'Pregunta' and (mensaje not like '¡Hola AVI, vengo del app!' or mensaje not like '¡Hola Interbank!' or mensaje not like '¡Hola, Avi Empresas!' or mensaje not like 'Hola, Avi Empresas' or mensaje not like 'Hola, AVI, vengo de la web y quiero saber dónde encuentro mi contraseña temporal a la BPI')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVISON HISOTORIA DE SALDOS - 09/02
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select * from avireportscomercial.conversaciones_avi_comercial
# MAGIC where session in (select distinct(session)
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and codigo = 'TRX_ACCOUNT_BALANCE_PEND')

# COMMAND ----------

# MAGIC %md
# MAGIC ## CUANTIFICACIÓN DE CONSULTAS A LOS FLUJOS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CUANTIFICACIÓN DE CONSULTAS
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select
# MAGIC count(distinct(session)),
# MAGIC month
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and codigo like 'DESA_%'
# MAGIC group by month
# MAGIC order by month

# COMMAND ----------

# MAGIC %md
# MAGIC ## CONSULTAS A LA OPCIÓN DE MOVIMIENTOS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CUANTIFICACIÓN DE CONSULTAS DE MOVIMIENTOS
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select
# MAGIC count(distinct(session)),
# MAGIC month
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and codigo = 'CNEG_001'
# MAGIC group by month
# MAGIC order by month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CONVERSACIONES QUE PASARON POR OTP
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select *
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where session in (select distinct(session)
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and mensaje like 'Hola,Avi%'

# COMMAND ----------

# MAGIC %md
# MAGIC ## CONVERSACIONES QUE VINIERON POR CANALES

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CONVERSACIONES QUE VINIERON POR CANALES 
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select *
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and month = 2 and codigo = 'Pregunta' and (mensaje like '¡Hola Avi Empresas!%' or mensaje like '¡Hola, Avi Empresas!%') --or mensaje like 'Hola, Avi Empresas%' or mensaje like 'Hola Avi Empresas%' or mensaje like 'Buen día Avi Empresas%' or mensaje like 'Buen día, Avi Empresas%')

# COMMAND ----------

# MAGIC %md
# MAGIC ## CONVERSACIONES DERIVACIONES CIMA CON UNTR

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select * from avireportscomercial.conversaciones_avi_comercial
# MAGIC where codigo like '%UNTR%' and session in (select distinct(session)
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where month >= 5 and year = 2024 and codigo = 'DERI_029' or codigo = 'DERI_027')
# MAGIC -- se incluye un like antes de las sesiones para incluir los untr

# COMMAND ----------

# MAGIC %md
# MAGIC # CONSULTAS: IMPULSO MI PERU

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select
# MAGIC year as ANO,
# MAGIC month as MES,
# MAGIC count(distinct(session)) as CANT_SESIONES
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where mensaje like '%my per%' and year in (2023,2024)
# MAGIC group by month, year
# MAGIC order by ANO asc, MES asc
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # CONSULTAS: CERTIFICADO DE NO ADEUDO

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select 
# MAGIC year as ANO,
# MAGIC month as MES,
# MAGIC count(distinct(session)) as CANT_SESIONES
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where mensaje like '%adeud%' and year in (2023,2024)
# MAGIC group by month, year
# MAGIC order by ANO asc, MES asc

# COMMAND ----------

# MAGIC %md
# MAGIC # CONSULTAS SESIONES OTP LUEGO DE CAIDAS

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select * from avireportscomercial.conversaciones_avi_comercial
# MAGIC where session in (select distinct(session)
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and month = 5 and registro in ('51953570573',
# MAGIC '51940196651',
# MAGIC '51998306116',
# MAGIC '51940196651',
# MAGIC '51920140534',
# MAGIC '51989952907',
# MAGIC '51993933220',
# MAGIC '51972960328',
# MAGIC '51954773294',
# MAGIC '51982596554',
# MAGIC '51988601217',
# MAGIC '51931094726',
# MAGIC '51949244699',
# MAGIC '51913544188',
# MAGIC '51950226033',
# MAGIC '51920191885',
# MAGIC '51973698506',
# MAGIC '51989726071',
# MAGIC '51950313120',
# MAGIC '51924495005',
# MAGIC '51992209621',
# MAGIC '51978788989',
# MAGIC '51926840288',
# MAGIC '51996066576',
# MAGIC '51971097921'
# MAGIC '51934500509',
# MAGIC '51990193185',
# MAGIC '51944261242',
# MAGIC '51991809567',
# MAGIC '51945441690',
# MAGIC '51936794859',
# MAGIC '51972778267',
# MAGIC '51953761771',
# MAGIC '51926453507',
# MAGIC '51933984769',
# MAGIC '51950411074',
# MAGIC '51992720898',
# MAGIC '51981434795',
# MAGIC '51984225589',
# MAGIC '51993485513'
# MAGIC )) and codigo != 'CAID_003' and codigo != 'CAID_005'

# COMMAND ----------

# MAGIC %md
# MAGIC # CONSULTA TOKEN MULTIEMPRESA : TOKEN FISICO

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select *
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where session in (select distinct(session)
# MAGIC from avireportscomercial.conversaciones_avi_comercial 
# MAGIC where year = 2024 and month in (4,5,6,7,8) and codigo in ('TOKE_009','DERI_019') and mensaje like '%C%')

# COMMAND ----------

# MAGIC %md
# MAGIC # CONSULTA DE CONTRASEÑA RECLAMO

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select*
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and month in (6,7,8) and session in ('d218460a-f0a8-4a7d-9f65-651b3f08ee4f',
# MAGIC '189c64b0-67cf-494a-ad1b-b195dbd7aab2',
# MAGIC 'fa37fa1a-75aa-4a41-836e-9eaa80c677f0',
# MAGIC '7e0322b2-e8b6-4e87-a6c5-fd7e4c224ad5',
# MAGIC 'c0335335-a07b-4531-997e-5b22fa4e56c8',
# MAGIC 'd950ec1f-d673-4b99-b2d1-8e55ad0e46c6',
# MAGIC '08435d5d-2d1c-49eb-ba30-3746a7b69dc9',
# MAGIC 'b54b7b16-a781-4c6b-9720-0cfa672d13f6',
# MAGIC 'dce80770-be7d-424c-ab3c-3b871db1e50a',
# MAGIC 'd4bf0dfd-a5cf-43ce-8823-d41a147073ae',
# MAGIC 'baf7ed16-0895-49a0-9006-50bec73b5e5b',
# MAGIC '1f1c008d-c5d9-4b91-bc30-2c5cb03cc1e3',
# MAGIC '4e2a2286-bfa0-4e5c-88be-a06d50b15c6f',
# MAGIC '336a972d-d463-40bb-9ee0-6ba2e7c99f35',
# MAGIC '6dc5bb14-f0d9-46aa-a2e5-a0c5f68257d4',
# MAGIC 'a5316a98-db0e-4e70-9156-d9f8bf8d25bd',
# MAGIC '992907c8-03d1-41a8-8705-96b47d981b00',
# MAGIC '20bc9953-410a-4ea3-8067-7bb7e38e466d',
# MAGIC 'e3cf1c48-b229-4360-9223-87ad61b98bda',
# MAGIC '56d6458f-772b-4533-a5b5-524b1e4e57a8',
# MAGIC '279cc421-dd9a-4f3b-ad1f-8a318ad5f5d3',
# MAGIC 'a7b8528b-fd19-4bbb-a64e-980984f843a2',
# MAGIC '88ba13bd-e6ee-4163-9913-95ecf8e37de1',
# MAGIC 'a5ac2a38-648c-409a-be8c-3dd5bd495e12',
# MAGIC '0622a3c5-64b9-4754-a480-a86a098d5122',
# MAGIC '45abb28e-4b71-4230-bcc8-596f49a010c9',
# MAGIC '6fd0aba9-327e-4009-91f7-5cdb8371ff68',
# MAGIC '2f4e21b8-e700-4692-9313-1755316c88b0',
# MAGIC '10976e25-a219-48cb-be34-03f76135b503',
# MAGIC 'faac6cf5-8802-420e-9552-9762bd446bc6',
# MAGIC 'f3cf1dc7-3238-4c33-bedc-3b77e31c13ce',
# MAGIC '371abee7-4774-4903-9ed1-3f2b20a3cc6f',
# MAGIC '51e13ddb-1d7f-4a45-a8ab-276b6386ef78',
# MAGIC '49d508f0-14cd-4350-8822-6f48f00dca19',
# MAGIC '2c7d7c82-d74c-46b7-8ae7-be8cf7e7818c',
# MAGIC '6b2aa112-e638-47dc-9d10-7df92d8f64fb',
# MAGIC '47b5642d-0994-49de-a6cf-1ec51c18f6d2',
# MAGIC '1545b303-dc99-4ee5-8a00-6f3efcfa0ce8',
# MAGIC 'e4be4df8-53e9-4e86-8525-f306944dca9f',
# MAGIC 'f8112a9c-a487-4f07-aeff-74cc117448f8',
# MAGIC 'b2b240b3-e5f5-4c8b-83f7-342f0b5d7f56',
# MAGIC '2db02f75-181a-4838-9d79-b98b4e29aadd',
# MAGIC '1b1de843-448a-4b7d-8e47-44edd5c224ec',
# MAGIC '7f606056-9cb0-49e7-ac42-986a9de078a0',
# MAGIC 'b5c4976e-144f-436f-8d5d-f79db8e236c3',
# MAGIC '5d01b2ea-12bd-4e08-b29c-6891ec85ebd0',
# MAGIC 'e62c3022-22e5-4299-a487-3077bbe1d07a',
# MAGIC 'f94fcc6f-d4ae-464c-a66a-c81a0319a5ca',
# MAGIC '43faa7d8-5f18-43f7-a237-85dbd9154c8a',
# MAGIC '767e5f84-2e48-46e7-bedc-8f8755f79ef7',
# MAGIC '17924401-f923-469d-89e2-c0d5a4b67117',
# MAGIC 'bc434963-a464-4401-955f-da004d6fb745',
# MAGIC 'e6905da8-53b1-4d97-add0-5070e87c6f69',
# MAGIC '13a05949-c9af-4a34-aebd-082db877a554',
# MAGIC '88d6363e-e2f0-4c87-884a-3b90e7e7610b',
# MAGIC 'e40a3dcb-ac2d-4e05-bdec-62f8404d5f0e',
# MAGIC '5a500191-b93a-46a4-a7be-20d8fcbb2a45')

# COMMAND ----------

# MAGIC %md
# MAGIC # CONVERSACIÓN INMEDIATA

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select *
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and month in (7,8) and registro in (51986826655,51944566990)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CONVERSACIONES DONDE LOS USUARIOS CONSULTARON "PAGUE"
# MAGIC REFRESH TABLE avireportscomercial.conversaciones_avi_comercial;
# MAGIC select * from avireportscomercial.conversaciones_avi_comercial
# MAGIC where session in (select distinct(session)
# MAGIC from avireportscomercial.conversaciones_avi_comercial
# MAGIC where year = 2024 and month = 11 and day in (1,2,3) and mensaje like '%asesor%')
