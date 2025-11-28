using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Web_Service
{
    
    public class OperacionSQL
    {
        public string WorkareaName { get; set; }
        public int Operacion { get; set; }
        public string Codigo { get; set; }
        public string Descripcion { get; set; }
        public string Tipo { get; set; }
        public string Deposito { get; set; }
        public string UnidadMedida { get; set; }
        public int IdXml { get; set; }
    }

    // Modelo para la jerarquía
    public class OperacionJerarquica
    {
        public string WorkareaName { get; set; }
        public int NumeroOperacion { get; set; }
        public string Codigo { get; set; }
        public string Descripcion { get; set; }
        public string Tipo { get; set; }
        public string Deposito { get; set; }
        public string UnidadMedida { get; set; }
        public int IdXml { get; set; }

        public OperacionJerarquica Padre { get; set; }
        public List<OperacionJerarquica> Hijos { get; set; } = new List<OperacionJerarquica>();
        public int Nivel { get; set; }
    }

    public class CampoValor
    {
        [JsonProperty("campo")]
        public string Campo { get; set; }

        [JsonProperty("valor")]
        public string Valor { get; set; }
    }

    public class EstructuraProducto
    {
        [JsonProperty("producto")]
        public string Producto { get; set; }

        [JsonProperty("qtdBase")]
        public string QtdBase { get; set; }

        [JsonProperty("estructura")]
        public List<List<CampoValor>> Estructura { get; set; } = new List<List<CampoValor>>();
    }
    public class PruebaSG1
    {
        public static string connectionString = "Data Source=DEPLM-07-PC\\SQLEXPRESS;Initial Catalog=AgrometalBop;User ID=sa;Password=infodba";
        // Consumir datos de SQL
        public static async Task<List<OperacionSQL>> ObtenerOperacionesAsync()
        {
            var operaciones = new List<OperacionSQL>();

            string query = @"
            WITH ProcessData AS (
    SELECT
        CAST(dbo.ProcessOccurrence.id_Table AS BIGINT) AS idTable, -- Changed INT to BIGINT
        COALESCE(dbo.Process.catalogueId, dbo.Operation.catalogueId) AS catalogueId,
        COALESCE(dbo.Process.name, dbo.Operation.name) AS name,
        CAST(dbo.ProcessOccurrence.parentRef AS BIGINT) AS ParentRef, -- Changed INT to BIGINT
        uud.value,
        COALESCE(dbo.Process.Subtype, dbo.Operation.Subtype) AS subtype,
        dbo.ProcessOccurrence.idXml,
		CASE WHEN LEFT(PRO.catalogueId,2) = 'P-' THEN SUBSTRING(PRO.catalogueId, 3,LEN(PRO.catalogueId)) ELSE
		CASE WHEN LEFT(PRO.catalogueId,2) = 'P0' THEN SUBSTRING(PRO.catalogueId, 2,LEN(PRO.catalogueId)) END
		END AS Abuelo
    FROM dbo.ProcessOccurrence
    LEFT JOIN dbo.ProcessRevision ON ProcessOccurrence.instancedRef = ProcessRevision.id_Table AND ProcessOccurrence.idXml = ProcessRevision.idXml
    LEFT JOIN dbo.Process ON Process.id_Table = ProcessRevision.masterRef AND ProcessRevision.idXml = Process.idXml
	LEFT JOIN dbo.ProcessOccurrence PO ON ProcessOccurrence.parentRef = PO.id_Table
	LEFT JOIN dbo.ProcessRevision PR ON PO.instancedRef = PR.id_Table
    LEFT JOIN dbo.Process PRO ON PRO.id_Table = PR.masterRef
    LEFT JOIN dbo.OperationRevision ON OperationRevision.id_Table = ProcessOccurrence.instancedRef AND ProcessOccurrence.idXml = OperationRevision.idXml
    LEFT JOIN dbo.Operation ON Operation.id_Table = OperationRevision.masterRef AND OperationRevision.idXml = Operation.idXml
   LEFT JOIN dbo.AssociatedAttachment AS attch 
    ON attch.id_Table = CAST(
        REPLACE(
            REPLACE(
                SUBSTRING(dbo.ProcessOccurrence.associatedAttachmentRefs, CHARINDEX('#id', dbo.ProcessOccurrence.associatedAttachmentRefs) + 3, LEN(ProcessOccurrence.associatedAttachmentRefs)),
                ' ', ''
            ),
            '#id', ''
        ) AS BIGINT -- Changed INT to BIGINT
    ) 
        AND attch.role = 'METimeAnalysisRelation'
        AND ProcessOccurrence.idXml = attch.idXml
    LEFT JOIN dbo.UserValue_UserData AS uud 
        ON uud.id_Father - 1 = CAST(REPLACE(attch.attachmentRef, '#id', '') AS BIGINT) -- Changed INT to BIGINT
        AND uud.title = 'allocated_time'
        AND attch.idXml = uud.idXml
),
RankedData AS (
SELECT 
		Process.catalogueId AS instancedProcess,
		WorkArea.catalogueId AS instancedWorkArea,
        WorkArea.name,
        PO.idXml,
        ROW_NUMBER() OVER (PARTITION BY WorkArea.catalogueId, PO.idXml ORDER BY PO.id_Table ASC) AS rn
  FROM Occurrence occ
  INNER JOIN ProcessOccurrence PO on PO.id_Table = occ.parentRef
  LEFT JOIN OperationRevision OPR on OPR.id_Table - 11 = occ.parentRef
  INNER JOIN dbo.ProcessRevision ON ProcessRevision.id_Table + 19 = OPR.id_Table
  LEFT JOIN dbo.Process ON ProcessRevision.masterRef = Process.id_Table
  LEFT JOIN dbo.Operation ON Operation.id_Table = OPR.masterRef
  JOIN dbo.WorkAreaRevision ON WorkAreaRevision.id_Table = occ.instancedRef
  JOIN dbo.WorkArea ON WorkAreaRevision.masterRef = WorkArea.id_Table
  WHERE
       occ.subType = 'MEWorkArea'),

RankedData2 AS (
SELECT 
		Process.catalogueId AS instancedProcess,
        Process.name AS Process_codigo,
        WorkArea.catalogueId AS instancedWorkArea,
        WorkArea.name,
        PO.idXml,
        ROW_NUMBER() OVER (PARTITION BY WorkArea.catalogueId, PO.idXml ORDER BY PO.id_Table DESC) AS rn
  FROM Occurrence occ
  INNER JOIN ProcessOccurrence PO on PO.id_Table = occ.parentRef
  LEFT JOIN OperationRevision OPR on OPR.id_Table - 11 = occ.parentRef
  INNER JOIN dbo.ProcessRevision ON ProcessRevision.id_Table + 19 = OPR.id_Table
  LEFT JOIN dbo.Process ON ProcessRevision.masterRef = Process.id_Table
  LEFT JOIN dbo.Operation ON Operation.id_Table = OPR.masterRef
  JOIN dbo.WorkAreaRevision ON WorkAreaRevision.id_Table = occ.instancedRef
  JOIN dbo.WorkArea ON WorkAreaRevision.masterRef = WorkArea.id_Table
  WHERE
       occ.subType = 'MEWorkArea')

SELECT 
    MEProcess.catalogueId AS Process_catalogueId,
    MEProcess.name AS Process_name,
     CASE WHEN TRY_CAST(COALESCE(ROUND(TRY_CAST(MEProcess.value AS decimal(18,2))/3600,2),0) AS nvarchar) = 0.0000000 THEN 0.1 ELSE
	TRY_CAST(COALESCE(ROUND(TRY_CAST(MEProcess.value AS decimal(18,2))/3600,2),0) AS nvarchar) END
	AS Tiempo,
    MEOP.catalogueId AS Operation_catalogueId,
    MEOP.name AS Operation_name,
    CASE WHEN rd.instancedWorkArea = '000485' THEN '481708' ELSE rd.instancedWorkArea END AS InstancedWorkArea,
    rd.name AS Workarea_name,
    rd.rn AS Operacion,
    CASE WHEN LEFT(COALESCE(rd2.instancedProcess, MEProcess.catalogueId),2) = 'P-' THEN 
	SUBSTRING(COALESCE(rd2.instancedProcess, MEProcess.catalogueId),3,LEN(COALESCE(rd2.instancedProcess, MEProcess.catalogueId)))
	ELSE COALESCE(rd2.instancedProcess, MEProcess.catalogueId) END
	AS codigo,
	--MEOP.catalogueId AS codigo,
    CONCAT(rd2.Process_codigo, ' (',MEProcess.Abuelo,')')  AS Descripcion,
    'PA' as tipo,
    '01' as deposito,
    'UN' AS 'Unidad de Medida',
    MEProcess.idXml
FROM ProcessData AS MEProcess
LEFT JOIN ProcessData AS MEOP
    ON MEProcess.idTable = MEOP.ParentRef
    AND MEProcess.subtype = 'MEProcess'
    AND MEOP.subtype = 'MEOP'
    AND MEProcess.idXml = MEOP.idXml
JOIN RankedData rd 
    ON rd.instancedProcess = MEProcess.catalogueId
    AND rd.idXml = MEProcess.idXml
LEFT JOIN RankedData2 rd2 
    ON rd2.instancedWorkArea = rd.instancedWorkArea 
    AND rd2.rn = 1
    AND rd2.idXml = rd.idXml
WHERE MEProcess.subtype = 'MEProcess'
ORDER BY MEProcess.idXml, COALESCE(rd2.instancedProcess, MEProcess.catalogueId), rd.rn DESC";

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                using (var command = new SqlCommand(query, connection))
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            operaciones.Add(new OperacionSQL
                            {
                                WorkareaName = reader["Workarea_name"].ToString(),
                                Operacion = Convert.ToInt32(reader["Operacion"]),
                                Codigo = reader["codigo"].ToString(),
                                Descripcion = reader["Descripcion"].ToString(),
                                Tipo = reader["tipo"].ToString(),
                                Deposito = reader["deposito"].ToString(),
                                UnidadMedida = reader["UnidadMedida"].ToString(),
                                IdXml = Convert.ToInt32(reader["idXml"])
                            });
                        }
                    }
                }
            }

            return operaciones;
        }

        // Convertir a jerarquía
        public static List<OperacionJerarquica> CrearJerarquia(List<OperacionSQL> operacionesSQL)
        {
            var operacionesJerarquicas = new List<OperacionJerarquica>();

            // Convertir a modelo jerárquico
            var operaciones = operacionesSQL.Select(op => new OperacionJerarquica
            {
                WorkareaName = op.WorkareaName,
                NumeroOperacion = op.Operacion,
                Codigo = op.Codigo,
                Descripcion = op.Descripcion,
                Tipo = op.Tipo,
                Deposito = op.Deposito,
                UnidadMedida = op.UnidadMedida,
                IdXml = op.IdXml
            }).ToList();

            // Agrupar por área de trabajo
            var gruposPorWorkarea = operaciones
                .GroupBy(op => op.WorkareaName)
                .ToList();

            foreach (var grupo in gruposPorWorkarea)
            {
                // Ordenar por número de operación descendente
                var operacionesOrdenadas = grupo
                    .OrderByDescending(op => op.NumeroOperacion)
                    .ToList();

                // Crear jerarquía consecutiva
                var raicesGrupo = CrearJerarquiaConsecutiva(operacionesOrdenadas);
                operacionesJerarquicas.AddRange(raicesGrupo);
            }

            return operacionesJerarquicas;
        }

        public static List<OperacionJerarquica> CrearJerarquiaConsecutiva(List<OperacionJerarquica> operaciones)
        {
            var raices = new List<OperacionJerarquica>();
            OperacionJerarquica ultimoPadre = null;

            foreach (var operacion in operaciones)
            {
                if (ultimoPadre == null)
                {
                    // Primera operación es raíz
                    operacion.Nivel = 0;
                    raices.Add(operacion);
                    ultimoPadre = operacion;
                }
                else
                {
                    // Verificar si es consecutiva
                    if (ultimoPadre.NumeroOperacion - operacion.NumeroOperacion == 1)
                    {
                        // Es hija directa
                        operacion.Padre = ultimoPadre;
                        operacion.Nivel = ultimoPadre.Nivel + 1;
                        ultimoPadre.Hijos.Add(operacion);
                        ultimoPadre = operacion;
                    }
                    else
                    {
                        // Buscar padre apropiado o crear nueva raíz
                        var padreEncontrado = BuscarPadreConsecutivo(raices, operacion.NumeroOperacion);

                        if (padreEncontrado != null)
                        {
                            operacion.Padre = padreEncontrado;
                            operacion.Nivel = padreEncontrado.Nivel + 1;
                            padreEncontrado.Hijos.Add(operacion);
                        }
                        else
                        {
                            operacion.Nivel = 0;
                            raices.Add(operacion);
                        }

                        ultimoPadre = operacion;
                    }
                }
            }

            return raices;
        }

        public static OperacionJerarquica BuscarPadreConsecutivo(List<OperacionJerarquica> raices, int numeroOperacion)
        {
            foreach (var raiz in raices)
            {
                var resultado = BuscarPadreConsecutivoRecursivo(raiz, numeroOperacion);
                if (resultado != null)
                    return resultado;
            }
            return null;
        }

        public static OperacionJerarquica BuscarPadreConsecutivoRecursivo(OperacionJerarquica nodo, int numeroOperacion)
        {
            if (nodo.NumeroOperacion == numeroOperacion + 1)
                return nodo;

            foreach (var hijo in nodo.Hijos)
            {
                var resultado = BuscarPadreConsecutivoRecursivo(hijo, numeroOperacion);
                if (resultado != null)
                    return resultado;
            }

            return null;
        }

        // Generar JSON con la estructura requerida - UN JSON POR CADA PADRE
        public static List<EstructuraProducto> GenerarEstructuraJSON(List<OperacionJerarquica> raices)
        {
            var estructuras = new List<EstructuraProducto>();

            // Recorrer TODA la jerarquía, no solo las raíces
            foreach (var raiz in raices)
            {
                RecorerJerarquiaYGenerarJSONs(raiz, estructuras);
            }

            return estructuras;
        }

        // Método recursivo que genera un JSON por cada nodo que tenga hijos
        public static void RecorerJerarquiaYGenerarJSONs(OperacionJerarquica nodo, List<EstructuraProducto> estructuras)
        {
            // Si este nodo tiene hijos, generar un JSON para él
            if (nodo.Hijos.Count > 0)
            {
                var estructura = new EstructuraProducto
                {
                    Producto = nodo.Codigo, // Este nodo es el "PADRE"
                    QtdBase = "1",
                    Estructura = new List<List<CampoValor>>()
                };

                // Agregar SOLO los hijos directos (no recursivo)
                foreach (var hijo in nodo.Hijos)
                {
                    var entradaHijo = new List<CampoValor>
                {
                    new CampoValor { Campo = "codigo", Valor = hijo.Codigo },
                    new CampoValor { Campo = "cantidad", Valor = "1" },
                    new CampoValor { Campo = "descripcion", Valor = hijo.Descripcion },
                    new CampoValor { Campo = "operacion", Valor = hijo.NumeroOperacion.ToString() },
                    new CampoValor { Campo = "workarea", Valor = hijo.WorkareaName }
                };

                    estructura.Estructura.Add(entradaHijo);
                }

                estructuras.Add(estructura);
            }

            // Recursivamente procesar los hijos
            foreach (var hijo in nodo.Hijos)
            {
                RecorerJerarquiaYGenerarJSONs(hijo, estructuras);
            }
        }

        public static async Task<string> GenerarJSONCompleto()
        {
            try
            {
                Console.WriteLine("Iniciando GenerarJSONCompleto...");

                // 1. Obtener datos de SQL
                Console.WriteLine("Obteniendo operaciones de SQL...");
                var operacionesSQL = await ObtenerOperacionesAsync();
                Console.WriteLine($"Operaciones obtenidas: {operacionesSQL.Count}");

                if (operacionesSQL.Count == 0)
                {
                    Console.WriteLine("ADVERTENCIA: No se obtuvieron operaciones de la base de datos");
                    return "[]"; // Retorna array vacío si no hay datos
                }

                // Mostrar algunas operaciones para debug
                Console.WriteLine("Primeras 3 operaciones:");
                for (int i = 0; i < Math.Min(3, operacionesSQL.Count); i++)
                {
                    var op = operacionesSQL[i];
                    Console.WriteLine($"  - WorkArea: {op.WorkareaName}, Operacion: {op.Operacion}, Codigo: {op.Codigo}");
                }

                // 2. Crear jerarquía
                Console.WriteLine("Creando jerarquía...");
                var jerarquia = CrearJerarquia(operacionesSQL);
                Console.WriteLine($"Raíces de jerarquía creadas: {jerarquia.Count}");

                // 3. Generar estructura JSON
                Console.WriteLine("Generando estructura JSON...");
                var estructuras = GenerarEstructuraJSON(jerarquia);
                Console.WriteLine($"Estructuras JSON generadas: {estructuras.Count}");

                // 4. Serializar a JSON
                Console.WriteLine("Serializando a JSON...");
                var json = JsonConvert.SerializeObject(estructuras, Formatting.Indented);

                Console.WriteLine($"JSON final generado (longitud: {json.Length} caracteres)");
                Console.WriteLine("JSON generado exitosamente!");

                return json;
            }
            catch (SqlException sqlEx)
            {
                Console.WriteLine($"Error de SQL: {sqlEx.Message}");
                Console.WriteLine($"Número de error SQL: {sqlEx.Number}");
                throw new Exception($"Error de base de datos: {sqlEx.Message}", sqlEx);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error general: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                throw new Exception($"Error al generar JSON: {ex.Message}", ex);
            }
        }

        // Método alternativo: generar una sola estructura por workarea
        public async Task<List<string>> GenerarJSONPorWorkarea()
        {
            var operacionesSQL = await ObtenerOperacionesAsync();
            var resultados = new List<string>();

            var gruposPorWorkarea = operacionesSQL
                .GroupBy(op => op.WorkareaName)
                .ToList();

            foreach (var grupo in gruposPorWorkarea)
            {
                var operacionesGrupo = grupo.ToList();
                var jerarquiaGrupo = CrearJerarquia(operacionesGrupo);
                var estructurasGrupo = GenerarEstructuraJSON(jerarquiaGrupo);

                var json = JsonConvert.SerializeObject(estructurasGrupo, Formatting.Indented);
                resultados.Add(json);
            }

            return resultados;
        }
    }
}
