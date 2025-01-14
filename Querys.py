import pandas as pd
import pyarrow
from sqlalchemy import create_engine

salessystem = create_engine(
    'mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
    ':3306/salessystem')

warehouse = create_engine(
    'postgresql://admindb:72656770@datawarehouse.cgvmexzrrsgs.us-east-1.rds.amazonaws.com'
    ':5432/warehouse')


bancarizar_emitidos = "SELECT numero_documento,ruc,(CASE  WHEN (SELECT fecha_cuota1 FROM acc._7 WHERE cui_relacionado=concat('5',cui)) IS null THEN fecha_emision ELSE (SELECT fecha_cuota1 FROM acc._7 WHERE cui_relacionado=concat('5',cui)) END),valor+igv AS total,CONCAT(numero_serie,'-',numero_correlativo) FROM acc._5 WHERE _5.tipo_comprobante=1 AND observaciones NOT LIKE '%ANULADA' AND numero_documento IN ('20611957476') AND periodo_tributario=202408 AND valor+igv>1999.99 ORDER BY numero_documento;"

catalogo = pd.read_sql('SELECT * FROM catalogo', salessystem)

pre_detalle =pd.read_sql('SELECT * FROM pre_detalle ORDER BY fecha_emision DESC LIMIT 200', warehouse)

detalle_completo = pd.merge(pre_detalle, catalogo, on='descripcion', how='left')

entidades = pd.read_sql('SELECT * FROM acc.entities ORDER BY ruc ASC', warehouse)

"""SELECT fecha,periodo_mensual,tipo_operacion,cantidad_presentacion  FROM acc.iqbf ORDER BY fecha,tipo_operacion;

SELECT
       nro_operacion,
       (CASE tipo_operacion WHEN 'USO' THEN '04'END) AS codigo_tipo_operacion,
       (CASE transaccion WHEN 'USO AL INTERIOR DEL NEGOCIO' THEN '154'END) AS codigo_transaccion,
       TO_CHAR(fecha, 'dd/mm/yyyy') AS fecha_transaccion,
       LEFT(datos_propietario,11) AS documento_propietario,
       COALESCE(NULLIF(tipo_servicio, ''), '01') AS indicador_establecimiento,
       LEFT(establecimiento,4) AS codigo_establecimiento,
       tipo_servicio AS documento_prestador_servicio,
       tipo_servicio,
       SPLIT_PART(presentacion, ' - ', 2) AS codigo_presentacion,
       cantidad_presentacion,
       merma,
       (CASE SPLIT_PART(documento, '-', 1) WHEN 'ORDEN DE PRODUCCIÓN' THEN '20'END) AS documento_asociado,
       TO_CHAR(fecha, 'yyyymmdd01') AS numero_documento,
       null AS indicador_produccion,
       null AS codigo_producto_nofiscalizado,
       null AS cantidad_producto_nofiscalizado,
       tipo_documento_precedente,
       nro_documento_precedente,
       (CASE SPLIT_PART(relacionado_bien, '-', 1) WHEN 'DNI' THEN '1'END) AS tipo_documento_relacionado,
       SPLIT_PART(relacionado_bien, '-', 2) AS nro_documento_relacionado,
       SPLIT_PART(relacionado_bien, '-', 3) AS nombre_relacionado,
       nro_matricula_transporte,
       nombre_transporte,
       equipo_maquinaria,
       placa_equipo_maquinaria,
       observaciones,
       null AS lastpipe
       FROM acc.iqbf WHERE estado = 'PENDIENTE' ORDER BY fecha,transaccion;


SELECT _5.ruc, _5.numero_documento, _5.numero_correlativo, _6.numero_documento_referencia, _6.descripcion, _6.cantidad, _6.precio_unitario FROM acc._5 LEFT JOIN acc._6 ON CONCAT('5',_5.cui) = _6.cui_relacionado WHERE _5.periodo_tributario = 202405 AND _6.tipo_operacion = 1 ORDER BY ruc, numero_correlativo;

SELECT * FROM (SELECT DISTINCT ON (verificador) _5.ruc, _5.numero_documento, _5.numero_correlativo, _6.numero_documento_referencia, (CASE WHEN SUBSTRING(_6.numero_documento_referencia,1,4) = 'EG' THEN LEFT(SUBSTRING(_6.numero_documento_referencia,10,8),-1)::INT ELSE 0 END) AS guia, _6.descripcion, _6.cantidad, _6.precio_unitario, _5.ruc::VARCHAR||(CASE WHEN SUBSTRING(_6.numero_documento_referencia,1,4) = 'EG07' THEN LEFT(SUBSTRING(_6.numero_documento_referencia,10,8),-1) ELSE '0' END) AS verificador FROM acc._5 LEFT JOIN acc._6 ON cui = SUBSTRING(cui_relacionado,2) WHERE _5.periodo_tributario = 202405 AND _6.subdiario = 5 AND _5.numero_documento NOT IN (SELECT DISTINCT _5.ruc::VARCHAR FROM acc._5) )AS provisional ORDER BY ruc, guia;

SELECT * FROM (SELECT DISTINCT ON (guia) _5.ruc, _5.numero_documento, _5.numero_correlativo, _6.numero_documento_referencia, (CASE WHEN SUBSTRING(_6.numero_documento_referencia,1,4) = 'EG07' THEN substring(_6.numero_documento_referencia FROM 8 FOR length(_6.numero_documento_referencia) - 8 - 1)::INT ELSE 0 END) AS guia, _6.descripcion, _6.cantidad, _6.precio_unitario FROM acc._5 LEFT JOIN acc._6 ON cui = SUBSTRING(cui_relacionado,2) WHERE _5.periodo_tributario = 202405 AND _6.subdiario = 5 AND _5.numero_documento NOT IN (SELECT DISTINCT _5.ruc::VARCHAR FROM acc._5) )AS provisional ORDER BY ruc, guia;

/*VISTA PARA DESCARGAR GUIAS DE REMISION, CONSULTA SOLO GUIAS EN ORDEN DE NUMERO ASCENDENTE AGRUPADOS POR RUC DE PROVEEDOR */
SELECT * FROM (SELECT DISTINCT ON (_6.cui_relacionado) _5.ruc, _5.numero_correlativo, TRIM('|' FROM SPLIT_PART(_6.numero_documento_referencia,' - ',2))::INT AS guia FROM acc._6 LEFT JOIN acc._5 ON _5.cui = SUBSTRING(_6.cui_relacionado,2) WHERE _5.periodo_tributario = 202406 AND _6.subdiario = 5 AND _5.numero_documento NOT IN ('20605962468','20606482753','20603826303','20610737553','20611198427','20606401842','20609753723','20601124506','20610428101','20611097400','20606283858') AND SUBSTRING(_6.numero_documento_referencia,1,2) = 'EG') AS provisional ORDER BY ruc, guia;

SELECT * FROM (SELECT DISTINCT ON (_6.cui_relacionado) _5.ruc AS proveedor, _5.fecha_emision, _5.numero_documento AS adquiriente, _5.numero_correlativo AS factura, _6.numero_documento_referencia AS guia, _6.cui_relacionado AS cui FROM acc._6 LEFT JOIN acc._5 ON _5.cui = SUBSTRING(_6.cui_relacionado,2) WHERE _5.periodo_tributario = 202405 AND _6.subdiario = 5 AND _5.numero_documento NOT IN (SELECT DISTINCT _5.ruc::VARCHAR FROM acc._5)) AS provisional ORDER BY adquiriente, proveedor, fecha_emision, factura;


/*VISTA SOLO FACTURAS NO ANULADAS DEL PERIODO SELECCIONADO*/
SELECT numero_documento, (SELECT nombre_razon FROM priv.entities WHERE _5.ruc=entities.ruc) AS proveedor, (CASE WHEN (SELECT _7.cui_relacionado FROM acc._7 WHERE _7.cui_relacionado = CONCAT('5',_5.cui)) IS NOT NULL THEN (SELECT _7.fecha_cuota1 FROM acc._7 WHERE _7.cui_relacionado = CONCAT('5',_5.cui)) ELSE fecha_emision END) AS fecha_pago, round(valor*1.18,2) AS importe, CONCAT(numero_serie,'-',numero_correlativo) AS documento_relacionado, (CASE WHEN (CASE WHEN (SELECT _7.cui_relacionado FROM acc._7 WHERE _7.cui_relacionado = CONCAT('5',_5.cui)) IS NOT NULL THEN (SELECT _7.fecha_cuota1 FROM acc._7 WHERE _7.cui_relacionado = CONCAT('5',_5.cui)) ELSE fecha_emision END) > '2024-06-10' THEN 'FECHA POSTERIOR' END) AS observaciones FROM acc._5 WHERE periodo_tributario = 202405 AND CONCAT(ruc,numero_serie,numero_correlativo) NOT IN (SELECT CONCAT(ruc,numero_serie_modificado,numero_correlativo_modificado) FROM acc._5 WHERE tipo_comprobante = 7) AND tipo_comprobante = 1 AND round(valor*1.18,2) > 2000 ORDER BY numero_documento;

/*VISTA SOLO FACTURAS ANULADAS DEL PERIODO SELECCIONADO*/
SELECT cui FROM acc._5 WHERE periodo_tributario = 202405 AND CONCAT(ruc,numero_serie,numero_correlativo) IN (SELECT CONCAT(ruc,numero_serie_modificado,numero_correlativo_modificado) FROM acc._5 WHERE tipo_comprobante = 7) AND tipo_comprobante = 1;

/*VERIFICAR SI HAY OPERACIONES DE VENTA O SALIDA ANULADAS EN TABLA _6*/
SELECT _6.cui_relacionado FROM acc._6 WHERE periodo_tributario = 202405 AND SUBSTRING(_6.cui_relacionado,2) IN (SELECT cui FROM acc._5 WHERE periodo_tributario = 202405 AND CONCAT(ruc,numero_serie,numero_correlativo) IN (SELECT CONCAT(ruc,numero_serie_modificado,numero_correlativo_modificado) FROM acc._5 WHERE tipo_comprobante = 7) AND tipo_comprobante = 1);

/*BORRAR OPERACIONES DE VENTA O SALIDA ANULADAS EN TABLA _6*/
DELETE FROM acc._6 WHERE periodo_tributario = 202405 AND SUBSTRING(_6.cui_relacionado,2) IN (SELECT cui FROM acc._5 WHERE periodo_tributario = 202405 AND CONCAT(ruc,numero_serie,numero_correlativo) IN (SELECT CONCAT(ruc,numero_serie_modificado,numero_correlativo_modificado) FROM acc._5 WHERE tipo_comprobante = 7) AND tipo_comprobante = 1);

CREATE OR REPLACE VIEW facturas_salessystem AS SELECT _6.periodo_tributario, _6.ruc AS alias, (CASE LEFT(_6.numero_documento_referencia,2) WHEN 'EG' THEN _6.numero_documento_referencia END) AS guia, _5.numero_serie AS serie, _5.numero_correlativo AS numero, _6.fecha AS emision, _5.numero_documento AS ruc, _5.tipo_moneda AS moneda, _6.descripcion, _6.unidad_medida AS unid_medida, _6.cantidad, _6.precio_unitario AS precio_unit, (CASE WHEN (SELECT _7.cui_relacionado FROM acc._7 WHERE _7.cui_relacionado = _6.cui_relacionado) IS NOT NULL THEN 'CREDITO' ELSE 'CONTADO' END) AS forma_pago, 'EMITIDO' AS estado, (CASE WHEN (SELECT _7.cui_relacionado FROM acc._7 WHERE _7.cui_relacionado = _6.cui_relacionado) IS NOT NULL THEN (SELECT _7.fecha_cuota1 FROM acc._7 WHERE _7.cui_relacionado = _6.cui_relacionado) END) AS vencimiento, (CASE WHEN (SELECT _7.cui_relacionado FROM acc._7 WHERE _7.cui_relacionado = _6.cui_relacionado) IS NOT NULL THEN (SELECT _7.importe_cuota1 FROM acc._7 WHERE _7.cui_relacionado = _6.cui_relacionado) END) AS cuota1, (CASE WHEN (SELECT _7.cui_relacionado FROM acc._7 WHERE _7.cui_relacionado = _6.cui_relacionado) IS NOT NULL THEN (SELECT _7.fecha_cuota2 FROM acc._7 WHERE _7.cui_relacionado = _6.cui_relacionado) END) AS vencimiento2, (CASE WHEN (SELECT _7.cui_relacionado FROM acc._7 WHERE _7.cui_relacionado = _6.cui_relacionado) IS NOT NULL THEN (SELECT _7.importe_cuota2 FROM acc._7 WHERE _7.cui_relacionado = _6.cui_relacionado) END) AS cuota2 FROM acc._6 JOIN acc._5 ON CONCAT('5',_5.cui) = _6.cui_relacionado WHERE _6.tipo_operacion = 1 ORDER BY numero_documento;



SELECT  numero_documento AS adquiriente, MIN(fecha_emision)-2 AS fecha_pedido, 202405 AS periodo, FLOOR(SUM(ROUND(_5.valor*1.18,2))) AS importe_total FROM acc._5 WHERE periodo_tributario = 202405 AND CONCAT(ruc,numero_serie,numero_correlativo) NOT IN (SELECT CONCAT(ruc,numero_serie_modificado,numero_correlativo_modificado) FROM acc._5 WHERE tipo_comprobante = 7) AND tipo_comprobante = 1 GROUP BY _5.numero_documento;
/*FUNCION DE ARRIBA PARA OBTENER PEDIDOS LISTA, SUBIR PEDIDOS Y CONECTAR SEGUN ADQUIRIENTE Y PERIODO POR ESTA OCASION*/

SELECT ruc FROM facturas_salessystem WHERE periodo_tributario = 202405 GROUP BY ruc;

/* CONSULTAR LISTA DE DETALLES DE CIERTO CLIENTE */
SELECT unidad_medida,descripcion,cantidad,precio_unitario, round(_5.valor*1.18,2), _5.numero_documento FROM acc._6 JOIN acc._5 ON _6.cui_relacionado = concat('5',_5.cui) WHERE _6.tipo_operacion=1 AND _5.numero_documento='10726501306' ORDER BY _5.fecha_emision DESC, _5.cui;

/* CONSULTAR LISTA DE DETALLES DE CIERTO PROVEEDOR PRECIO AJUSTADO Y CONSIDERANDO TIPO DE CAMBIO */
SELECT acc._6.unidad_medida,acc._6.descripcion,acc._6.cantidad,(CASE acc._5.tipo_moneda WHEN 'PEN' THEN acc._6.precio_unitario*0.95 WHEN 'USD' THEN (SELECT usd_s FROM public.tc WHERE acc._5.fecha_emision=tc.fecha_sunat) * acc._6.precio_unitario*0.95 END) AS precio_ajustado, round(_5.valor*1.18,2), _5.numero_documento FROM acc._6 JOIN acc._5 ON _6.cui_relacionado = concat('5',_5.cui) WHERE _6.tipo_operacion=1 AND _5.ruc='20611957476' ORDER BY _5.fecha_emision;

CREATE OR REPLACE VIEW facturas_noanuladas AS SELECT DISTINCT _5.*, _6.tipo_documento_referencia, _6.numero_documento_referencia FROM acc._5 left JOIN acc._6 ON _5.cui = SUBSTRING(_6.cui_relacionado,2) WHERE _5.observaciones NOT LIKE '%ANULADA' AND _5.tipo_comprobante = 1;

SELECT ruc, numero_correlativo from acc._5 where periodo_tributario = 202407 and numero_documento = '20609315173' order by ruc;

SELECT numero_documento,ruc,(CASE WHEN (SELECT fecha_cuota1 FROM acc._7 WHERE cui_relacionado = concat('5',_5.cui)) IS NULL
                                      THEN fecha_emision ELSE (SELECT fecha_cuota1 FROM acc._7 WHERE cui_relacionado = concat('5',_5.cui)) END),valor+igv,concat(numero_serie,'-',numero_correlativo)FROM acc._5 WHERE numero_documento IN ('20611210656','20425035844','20503365571','20601616051','20602448071','10068122762','20601004667','20425035844')and periodo_tributario = 202406 order by numero_documento;

SELECT DISTINCT numero_documento FROM acc._5 WHERE periodo_tributario > 202405 order by numero_documento

/*SELECCIONAR PEDIDOS POR ADQUIRIENTE PARA BANCARIZAR */
SELECT numero_documento,ruc,(CASE  WHEN (SELECT fecha_cuota1 FROM acc._7 WHERE cui_relacionado=concat('5',cui)) IS null THEN fecha_emision ELSE (SELECT fecha_cuota1 FROM acc._7 WHERE cui_relacionado=concat('5',cui)) END),valor+igv AS total,CONCAT(numero_serie,'-',numero_correlativo) FROM acc._5 WHERE _5.tipo_comprobante=1 AND observaciones NOT LIKE '%ANULADA' AND numero_documento IN ('20611957476') AND periodo_tributario=202408 AND valor+igv>1999.99 ORDER BY numero_documento;

SELECT DISTINCT descripcion, unidad_medida, cantidad, precio_unitario FROM acc._6
"""