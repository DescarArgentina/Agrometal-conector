using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Diagnostics;

namespace Web_Service
{
    public class DataModel
    {
        public string ParentName { get; set; }
        public string ParentCodigo { get; set; }
        public string ChildName { get; set; }
        public string ChildCodigo { get; set; }
        public string CantidadHijo { get; set; }
        public string Variante { get; set; }
    }


    public class Tabla_SG1
    {
        private readonly HttpClient _httpClient;

        // Productos que en la query original venían sin Codigo_Padre
        private static readonly HashSet<string> ProductosSinCodigoPadre =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public Tabla_SG1()
        {
            _httpClient = new HttpClient();
        }
        // ✅ Query original (la que ya tenés) — SIN CAMBIOS
        private const string consultaSG1_BOP_ConWorkArea = @"
WITH FirstProcessName AS (
    SELECT TOP (1)
        CASE WHEN LEN(p.catalogueId) != 8 THEN RIGHT(p.catalogueId, LEN(p.catalogueId) - 2)
        ELSE RIGHT(p.catalogueId, 6) END AS first_process_name
    FROM Process p
    LEFT JOIN ProcessRevision pr
        ON pr.masterRef = p.id_Table
    LEFT JOIN ProcessOccurrence po
        ON pr.id_Table = po.instancedRef
    WHERE po.parentRef IS NULL
),
ValidChildren AS (
    SELECT
        po.id_Table AS PR_OccId,
        CASE
            WHEN LEFT(prod.productId, 1) IN ('M','E')
                THEN RIGHT(prod.productId, LEN(prod.productId) - 1)
            WHEN RIGHT(prod.productId, 3) = '-FV'
                THEN LEFT(prod.productId, LEN(prod.productId) - 3)
            ELSE prod.productId
        END AS Codigo,
        prod_rev.subType AS subType,
        CASE
            WHEN prod.productId IS NULL THEN 0
            WHEN prod_rev.subType IN ('Agm4_MatPrimaRevision', 'Agm4_RepCompradoRevision')
                THEN ISNULL(
                        TRY_CAST(NULLIF(LTRIM(RTRIM(uv_qty.value)), '') AS FLOAT),
                        0
                     )
            ELSE 1
        END AS Qty
    FROM Process p
    LEFT JOIN ProcessRevision pr
        ON pr.masterRef = p.id_Table
    LEFT JOIN ProcessOccurrence po
        ON po.instancedRef = pr.id_Table
    LEFT JOIN ProcessOccurrence po_op
        ON po_op.parentRef = po.id_Table
    LEFT JOIN Occurrence o
        ON o.parentRef = po_op.id_Table
       AND o.subType NOT IN ('METool')
    LEFT JOIN ProductRevision prod_rev
        ON prod_rev.id_Table = o.instancedRef
    LEFT JOIN Product prod
        ON prod.id_Table = prod_rev.masterRef
    LEFT JOIN UserValue_UserData uv_qty
        ON uv_qty.id_Father = o.id_Table + 2
       AND uv_qty.title = 'Quantity'
       AND uv_qty.idXml  = o.idXml
    WHERE
        prod.productId IS NOT NULL
        AND NOT (
            prod_rev.subType = 'Mfg0MEFactoryToolRevision'
            OR
            (
                prod_rev.subType IN ('Agm4_MatPrimaRevision','Agm4_RepCompradoRevision')
                AND (
                    uv_qty.value IS NULL
                    OR LTRIM(RTRIM(uv_qty.value)) = ''
                    OR TRY_CAST(NULLIF(LTRIM(RTRIM(uv_qty.value)),'') AS FLOAT) = 0
                )
            )
        )
)
SELECT
    fpn.first_process_name AS Process_codigo,
    CASE
        WHEN LEFT(p.catalogueId, 1) IN ('M','E')
            THEN RIGHT(p.catalogueId, LEN(p.catalogueId) - 1)
        WHEN RIGHT(p.catalogueId, 3) = '-FV'
            THEN LEFT(p.catalogueId, LEN(p.catalogueId) - 3)
        WHEN LEFT(p.catalogueId, 2) = 'P-'
            THEN RIGHT(p.catalogueId, LEN(p.catalogueId) - 2)
        ELSE p.catalogueId
    END AS PR_Codigo,
    vc.Codigo AS Codigo,
    SUM(ISNULL(vc.Qty, 0)) AS Cantidad,
    vc.subType AS subType,
    uud2.value AS num_busqueda,
    wa_pick.WorkArea_CatalogueId,
    wa_pick.WorkArea_Nombre,
    wa_pick.WorkArea_Revision
FROM Process p
LEFT JOIN ProcessRevision pr
    ON pr.masterRef = p.id_Table
LEFT JOIN ProcessOccurrence po
    ON po.instancedRef = pr.id_Table
OUTER APPLY (
    SELECT TOP (1)
        wa2.catalogueId AS WorkArea_CatalogueId,
        wa2.name        AS WorkArea_Nombre,
        war2.revision   AS WorkArea_Revision
    FROM Occurrence owa
    JOIN WorkAreaRevision war2
        ON war2.id_Table = owa.instancedRef
    JOIN WorkArea wa2
        ON wa2.id_Table = war2.masterRef
    WHERE
        owa.subType IN ('MEWorkarea', 'MEWorkArea')
        AND owa.idXml = po.idXml
        AND (
            owa.parentRef = po.id_Table
            OR
            owa.parentRef IN (
                SELECT po_op2.id_Table
                FROM ProcessOccurrence po_op2
                WHERE po_op2.parentRef = po.id_Table
                  AND po_op2.idXml = po.idXml
            )
        )
    ORDER BY
        CASE WHEN wa2.name LIKE '%TERCEROS%' THEN 0 ELSE 1 END,
        owa.id_Table
) wa_pick
LEFT JOIN UserValue_UserData uud2
    ON uud2.id_Father = po.id_Table + 2
   AND uud2.title = 'SequenceNumber'
   AND uud2.idXml  = po.idXml
LEFT JOIN ValidChildren vc
    ON vc.PR_OccId = po.id_Table
CROSS JOIN FirstProcessName fpn
WHERE
    fpn.first_process_name <>
    CASE
        WHEN LEFT(p.catalogueId, 1) IN ('M','E')
            THEN RIGHT(p.catalogueId, LEN(p.catalogueId) - 1)
        WHEN RIGHT(p.catalogueId, 3) = '-FV'
            THEN LEFT(p.catalogueId, LEN(p.catalogueId) - 3)
        WHEN LEFT(p.catalogueId, 2) = 'P-'
            THEN RIGHT(p.catalogueId, LEN(p.catalogueId) - 2)
        ELSE p.catalogueId
    END
GROUP BY
    fpn.first_process_name,
    p.catalogueId,
    vc.Codigo,
    vc.subType,
    uud2.value,
    wa_pick.WorkArea_CatalogueId,
    wa_pick.WorkArea_Nombre,
    wa_pick.WorkArea_Revision
ORDER BY
    num_busqueda DESC,
    LEFT(p.catalogueId, 3) DESC;
";
        // ✅ Query alternativa: sin WorkArea / WorkAreaRevision / OUTER APPLY
        private const string consultaSG1_BOP_SinWorkArea = @"
WITH CTE_Hierarchy AS (
    SELECT DISTINCT
        o.id_Table,
        pr.name,
        CASE 
            WHEN LEFT(p.catalogueId, 2) = 'P-'
                THEN RIGHT(p.catalogueId, LEN(p.catalogueId) - 2)
            ELSE p.catalogueId
        END AS catalogueId,
        CAST(o.parentRef AS INT) AS parentRef,
        pr.revision,
        pr.subType,
        o.idXml,

        -- columnas alineadas con SB1
        'PA' AS Tipo,
        '01' AS Deposito,
        CASE
            WHEN uudUnidad.title = 'Agm4_Unidad'     THEN uudUnidad.value
            WHEN uudUnidad.title = 'Agm4_Kilogramos' THEN uudUnidad.value
            WHEN uudUnidad.title = 'Agm4_Litros'     THEN uudUnidad.value
            WHEN uudUnidad.title = 'Agm4_Metros'     THEN uudUnidad.value
            ELSE 'UN'
        END AS unMedida
    FROM ProcessOccurrence o
    INNER JOIN ProcessRevision pr ON o.instancedRef = pr.id_Table
    INNER JOIN Process p          ON pr.masterRef   = p.id_Table

    -- Unidad de medida
    LEFT JOIN Form fUnidad
        ON p.catalogueId = CASE
            WHEN CHARINDEX('/', fUnidad.name) > 0
                THEN LEFT(fUnidad.name, CHARINDEX('/', fUnidad.name) - 1)
            ELSE fUnidad.name
        END
    LEFT JOIN UserValue_UserData uudUnidad
        ON fUnidad.id_Table + 9 = uudUnidad.id_Father
       AND p.idXml = uudUnidad.idXml
),
CTE_Niveles AS (
    -- Nivel 0 (raíces)
    SELECT
        h.*,
        0 AS Nivel
    FROM CTE_Hierarchy h
    WHERE h.parentRef IS NULL OR h.parentRef = 0

    UNION ALL

    -- Niveles siguientes
    SELECT
        h.*,
        n.Nivel + 1
    FROM CTE_Hierarchy h
    INNER JOIN CTE_Niveles n ON h.parentRef = n.id_Table
)

-- PRIMERA PARTE: jerarquía de procesos / PR
SELECT
    COALESCE(Parent.name, '')        AS Nombre_Padre,
    COALESCE(Parent.catalogueId, '') AS Process_codigo,

    CASE
        WHEN LEFT(Child.catalogueId, 2) = 'PR'
            THEN Child.name + ' - Proceso: ' + COALESCE(Parent.catalogueId, '')
        ELSE Child.name
    END AS Nombre_Hijo,

    Child.catalogueId  AS Codigo_Hijo,
    Child.subType      AS Subtype_Hijo,
    Child.revision     AS Revision,
    1                  AS CantidadHijo_Total,
    nChild.Nivel       AS Nivel,

    -- Tipo calculado (SIN WorkArea)
    CASE
        -- PR de servicio explícito: PRxxxxxT
        WHEN LEFT(Child.catalogueId, 2) = 'PR'
         AND RIGHT(Child.catalogueId, 1) = 'T'
            THEN 'SV'

        -- Comprado o materia prima
        WHEN Child.subType IN ('Agm4_RepCompradoRevision','Agm4_MatPrimaRevision')
            THEN 'MP'

        -- Producto intermedio (PR)
        WHEN LEFT(Child.catalogueId, 2) = 'PR'
            THEN 'PI'

        -- Default
        ELSE 'PA'
    END AS Tipo,

    Child.Deposito,
    Child.unMedida
FROM CTE_Niveles nChild
LEFT JOIN CTE_Niveles  nParent ON nChild.parentRef = nParent.id_Table
LEFT JOIN CTE_Hierarchy Parent  ON nParent.id_Table = Parent.id_Table
LEFT JOIN CTE_Hierarchy Child   ON nChild.id_Table  = Child.id_Table

UNION ALL

-- SEGUNDA PARTE: MEConsumed + MEResource (SIN WorkArea)
SELECT
    -- Para recurso, el ""padre"" lógico es el PR (nombre del proceso PR).
    -- Para consumed, el padre lógico sigue siendo la Operación.
    CASE 
        WHEN occ.subType = 'MEResource' THEN prPR.name
        ELSE Operation.name
    END AS Nombre_Padre,

    pPR.catalogueId AS Process_codigo,

    CASE
        WHEN LEFT(x.CodigoNorm, 2) = 'PR'
            THEN p.name + ' - Proceso: ' + COALESCE(pPR.catalogueId, '')
        ELSE p.name
    END AS Nombre_Hijo,

    x.CodigoNorm AS Codigo_Hijo,

    pr.subType   AS Subtype_Hijo,
    pr.revision  AS Revision,
    COUNT(p.productId) AS CantidadHijo_Total,
    3            AS Nivel,

    CASE
        WHEN LEFT(x.CodigoNorm, 2) = 'PR'
         AND RIGHT(x.CodigoNorm, 1) = 'T'
            THEN 'SV'
        WHEN pr.subType IN ('Agm4_RepCompradoRevision','Agm4_MatPrimaRevision')
            THEN 'MP'
        WHEN LEFT(x.CodigoNorm, 2) = 'PR'
            THEN 'PI'
        ELSE 'PA'
    END AS Tipo,

    '01' AS Deposito,

    MAX(
        CASE
            WHEN uudUnidad2.title = 'Agm4_Unidad'     THEN uudUnidad2.value
            WHEN uudUnidad2.title = 'Agm4_Kilogramos' THEN uudUnidad2.value
            WHEN uudUnidad2.title = 'Agm4_Litros'     THEN uudUnidad2.value
            WHEN uudUnidad2.title = 'Agm4_Metros'     THEN uudUnidad2.value
            ELSE 'UN'
        END
    ) AS unMedida

FROM Occurrence occ
INNER JOIN ProductRevision pr ON pr.id_Table = occ.instancedRef
INNER JOIN Product p          ON p.id_Table  = pr.masterRef

-- o = ProcessOccurrence al que cuelga la Occurrence (para consumed: operación, para resource: PR)
LEFT JOIN ProcessOccurrence o 
       ON occ.parentRef = o.id_Table

-- Operación (solo aplica bien cuando occ = MEConsumed)
LEFT JOIN OperationRevision op ON op.id_Table = o.instancedRef
LEFT JOIN Operation          ON Operation.id_Table = op.masterRef

-- o2 = padre de o (para consumed: PR; para resource: Root)
LEFT JOIN ProcessOccurrence o2 ON o2.id_Table = o.parentRef

-- Elegimos el ProcessOccurrence del PR según subtype:
-- - MEConsumed  -> PR = o2
-- - MEResource  -> PR = o
LEFT JOIN ProcessOccurrence poPR
       ON poPR.id_Table = CASE 
                             WHEN occ.subType = 'MEResource' THEN o.id_Table
                             ELSE o2.id_Table
                           END

LEFT JOIN ProcessRevision prPR ON poPR.instancedRef = prPR.id_Table
LEFT JOIN Process         pPR  ON prPR.masterRef    = pPR.id_Table

-- Unidad de medida (del PR)
LEFT JOIN Form fUnidad2
    ON pPR.catalogueId = CASE
        WHEN CHARINDEX('/', fUnidad2.name) > 0
            THEN LEFT(fUnidad2.name, CHARINDEX('/', fUnidad2.name) - 1)
        ELSE fUnidad2.name
    END
LEFT JOIN UserValue_UserData uudUnidad2
    ON fUnidad2.id_Table + 9 = uudUnidad2.id_Father
   AND pPR.idXml = uudUnidad2.idXml

-- Código hijo normalizado
CROSS APPLY (
    SELECT CASE
        WHEN LEFT(p.productId, 1) = 'E'
            THEN RIGHT(p.productId, LEN(p.productId) - 1)
        ELSE p.productId
    END AS CodigoNorm
) AS x

WHERE occ.subType IN ('MEConsumed', 'MEResource')

GROUP BY
    occ.subType,
    Operation.name,
    prPR.name,
    pPR.catalogueId,
    p.name,
    x.CodigoNorm,
    pr.subType,
    pr.revision
ORDER BY
    Nivel,
    Codigo_Hijo DESC;
";

        private static string ObtenerConsultaSG1_BOP(SqlConnection connection)
        {
            bool existeWorkArea = false;
            bool existeWorkAreaRevision = false;
            bool hayMEWorkArea = false;
            bool esWorkAreaNormal = false; // MEWorkArea -> WorkAreaRevision

            // ¿Existen las tablas?
            using (var cmd = new SqlCommand("SELECT OBJECT_ID('WorkArea', 'U');", connection))
                existeWorkArea = cmd.ExecuteScalar() is not null and not DBNull;

            using (var cmd = new SqlCommand("SELECT OBJECT_ID('WorkAreaRevision', 'U');", connection))
                existeWorkAreaRevision = cmd.ExecuteScalar() is not null and not DBNull;

            // Si no existen, no podemos usar la query con WA
            if (!existeWorkArea || !existeWorkAreaRevision)
            {
                Utilidades.EscribirEnLog($"SG1 -> ObtenerConsultaSG1_BOP: WorkArea={existeWorkArea}, WorkAreaRevision={existeWorkAreaRevision}. Se elige SIN WorkArea.");
                return consultaSG1_BOP_SinWorkArea;
            }

            // ¿Hay nodos MEWorkArea en esta BD?
            using (var cmd = new SqlCommand(@"
        SELECT TOP 1 1
        FROM Occurrence o
        WHERE o.subType IN ('MEWorkArea','MEWorkarea');", connection))
            {
                hayMEWorkArea = cmd.ExecuteScalar() is not null and not DBNull;
            }

            if (!hayMEWorkArea)
            {
                Utilidades.EscribirEnLog("SG1 -> ObtenerConsultaSG1_BOP: No hay Occurrence MEWorkArea. Se elige SIN WorkArea.");
                return consultaSG1_BOP_SinWorkArea;
            }

            // Caso normal: MEWorkArea apunta a WorkAreaRevision
            using (var cmd = new SqlCommand(@"
        SELECT TOP 1 1
        FROM Occurrence o
        JOIN WorkAreaRevision war ON war.id_Table = o.instancedRef
        WHERE o.subType IN ('MEWorkArea','MEWorkarea');", connection))
            {
                esWorkAreaNormal = cmd.ExecuteScalar() is not null and not DBNull;
            }

            Utilidades.EscribirEnLog(
                $"SG1 -> ObtenerConsultaSG1_BOP: existeWA={existeWorkArea}, existeWAR={existeWorkAreaRevision}, hayMEWorkArea={hayMEWorkArea}, normal={esWorkAreaNormal}. " +
                $"Query={(esWorkAreaNormal ? "CON WorkArea" : "SIN WorkArea")}");

            return esWorkAreaNormal ? consultaSG1_BOP_ConWorkArea : consultaSG1_BOP_SinWorkArea;
        }

        public static async Task postSG1(Dictionary<string, List<List<Dictionary<string, string>>>> estructuras)
        {
            string url = "http://119.8.73.193:8086/rest/TCEstructura/Incluir/";
            string username = "USERREST";
            string password = "restagr";

            var putFallBack = new Dictionary<string, List<List<Dictionary<string, string>>>>();

            Console.WriteLine($"[SG1-POST] Iniciando envío masivo a Totvs. Cantidad de productos: {estructuras.Count}");
            Utilidades.EscribirEnLog($"[SG1-POST] Iniciando envío masivo a Totvs. Cantidad de productos: {estructuras.Count}");

            using (HttpClient client = new HttpClient())
            {
                var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
                client.DefaultRequestHeaders.Authorization =
                    new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

                foreach (var parent in estructuras)
                {
                    string producto = parent.Key;
                    int cantItems = parent.Value?.Count ?? 0;

                    Console.WriteLine($"[SG1-POST] Preparando envío para producto {producto} con {cantItems} ítems de estructura...");


                    var jsonBody = new
                    {
                        producto = producto,
                        qtdBase = "1",
                        estructura = parent.Value
                    };

                    string jsonData = JsonConvert.SerializeObject(jsonBody, Formatting.Indented);

                    // LOG: preview del JSON
                    Console.WriteLine($"[SG1-POST] JSON generado para producto {producto} (primeros 800 caracteres):");
                    Console.WriteLine(jsonData.Length > 800 ? jsonData.Substring(0, 800) + "..." : jsonData);

                    // Si quisieras ver TODO el JSON, ya lo tenés arriba, solo ojo con el volumen.
                    Utilidades.EscribirEnLog($"[SG1-POST] JSON COMPLETO para producto {producto}:\n{jsonData}");

                    // Obtener el código desde el propio objeto (no hace falta reparsear, pero lo dejo como lo tenés)
                    JObject obj = JObject.Parse(jsonData);
                    string? codigo = obj["producto"]?.ToString();

                    if (string.IsNullOrEmpty(codigo))
                    {
                        Console.WriteLine("[SG1-POST] ERROR: No se pudo obtener el código del producto, se omite este envío.");
                        continue;
                    }

                    Console.WriteLine($"[SG1-POST] Enviando POST para producto {codigo}...");
                    Utilidades.EscribirEnLog($"[SG1-POST] Enviando POST para producto {codigo}...");

                    int statusCode = 0;
                    string responseData = string.Empty;

                    try
                    {
                        var content = new StringContent(jsonData, Encoding.UTF8, "application/json");
                        HttpResponseMessage response = await client.PostAsync(url, content);

                        statusCode = (int)response.StatusCode;
                        responseData = await response.Content.ReadAsStringAsync();

                        if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                        {
                            Console.WriteLine($"[SG1-POST] POST devolvió 409 (Conflict) para {codigo}. Se acumula para PUT.");
                            Utilidades.EscribirEnLog($"[SG1-POST] POST devolvió 409 (Conflict) para {codigo}. Se acumula para PUT.");
                            putFallBack[codigo] = parent.Value;
                        }
                        else if (response.IsSuccessStatusCode)
                        {
                            Console.WriteLine($"[SG1-POST] POST OK para {codigo}: {statusCode} - {responseData}");
                            Utilidades.EscribirEnLog($"[SG1-POST] POST OK para {codigo}: {statusCode} - {responseData}");
                        }
                        else
                        {
                            Console.WriteLine($"[SG1-POST] POST ERROR para {codigo}: {statusCode} {response.ReasonPhrase}. Contenido: {responseData}");
                            Utilidades.EscribirEnLog($"[SG1-POST] POST ERROR para {codigo}: {statusCode} {response.ReasonPhrase}. Contenido: {responseData}");
                        }

                        Console.WriteLine($"[SG1-POST] Respuesta cruda para producto {codigo}: {responseData}, status {statusCode}");
                        Utilidades.EscribirEnLog($"[SG1-POST] Respuesta cruda para producto {codigo}: {responseData}, status {statusCode}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[SG1-POST] EXCEPCIÓN al enviar producto {producto}: {ex.Message}");
                        Utilidades.EscribirEnLog($"[SG1-POST] EXCEPCIÓN al enviar producto {producto}: {ex.Message}");
                    }
                    finally
                    {
                        ActualizarBase(statusCode, responseData, codigo);
                    }
                }
            }

            if (putFallBack.Count > 0)
            {
                Console.WriteLine($"[SG1-POST] Finalizado POST. Hay {putFallBack.Count} productos con 409 → se dispara PUT masivo.");
                Utilidades.EscribirEnLog($"[SG1-POST] Finalizado POST. Hay {putFallBack.Count} productos con 409 → se dispara PUT masivo.");
                await putSG1(putFallBack);
            }
            else
            {
                Utilidades.EscribirEnLog("[SG1-POST] Finalizado POST. No hay 409 para procesar con PUT.");
                Console.WriteLine("[SG1-POST] Finalizado POST. No hay 409 para procesar con PUT.");
            }
        }



        public async Task postSG1(string jsonString, Dictionary<string, List<List<Dictionary<string, string>>>>? putAcumulador = null)
        {
            string url = "http://119.8.73.193:8086/rest/TCEstructura/Incluir/";
            string username = "USERREST";
            string password = "restagr";

            bool esAcumuladorPropio = false;
            if (putAcumulador == null)
            {
                putAcumulador = new Dictionary<string, List<List<Dictionary<string, string>>>>();
                esAcumuladorPropio = true;
            }

            Console.WriteLine("[SG1-POST-UNIT] Iniciando postSG1(string).");

            using (HttpClient client = new HttpClient())
            {
                var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
                client.DefaultRequestHeaders.Authorization =
                    new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

                try
                {
                    JObject obj = JObject.Parse(jsonString);
                    string? codigo = obj["producto"]?.ToString();

                    if (string.IsNullOrEmpty(codigo))
                    {
                        Console.WriteLine("[SG1-POST-UNIT] ERROR: No se pudo obtener el código del producto del JSON.");
                        Console.WriteLine($"[SG1-POST-UNIT] JSON recibido:\n{jsonString}");
                        return;
                    }

                    Console.WriteLine($"[SG1-POST-UNIT] Código del producto: {codigo}");
                    Utilidades.EscribirEnLog($"[SG1-POST-UNIT] Código del producto: {codigo}");
                    Console.WriteLine("[SG1-POST-UNIT] JSON a enviar (primeros 800 caracteres):");
                    Console.WriteLine(jsonString.Length > 800 ? jsonString.Substring(0, 800) + "..." : jsonString);

                    int statusCode = 0;
                    string responseData = string.Empty;

                    try
                    {
                        var content = new StringContent(jsonString, Encoding.UTF8, "application/json");
                        HttpResponseMessage response = await client.PostAsync(url, content);

                        statusCode = (int)response.StatusCode;
                        responseData = await response.Content.ReadAsStringAsync();

                        if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                        {
                            Console.WriteLine($"[SG1-POST-UNIT] POST devolvió 409 para {codigo}. Se acumula para PUT.");
                            Utilidades.EscribirEnLog($"[SG1-POST-UNIT] POST devolvió 409 para {codigo}. Se acumula para PUT.");
                            var entrada = ConvertirJsonAEstructura(jsonString);
                            putAcumulador[entrada.codigo] = entrada.estructura;
                        }
                        else if (response.IsSuccessStatusCode)
                        {
                            Console.WriteLine($"[SG1-POST-UNIT] POST OK para {codigo}: {statusCode} - {responseData}");
                            Utilidades.EscribirEnLog($"[SG1-POST-UNIT] POST OK para {codigo}: {statusCode} - {responseData}");
                        }
                        else
                        {
                            Console.WriteLine($"[SG1-POST-UNIT] POST ERROR para {codigo}: {statusCode} {response.ReasonPhrase}. Contenido: {responseData}");
                            Utilidades.EscribirEnLog($"[SG1-POST-UNIT] POST ERROR para {codigo}: {statusCode} {response.ReasonPhrase}. Contenido: {responseData}");
                        }

                        Console.WriteLine($"[SG1-POST-UNIT] Respuesta para producto {codigo}: {responseData}, status {statusCode}");
                        Utilidades.EscribirEnLog($"[SG1-POST-UNIT] Respuesta para producto {codigo}: {responseData}, status {statusCode}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[SG1-POST-UNIT] EXCEPCIÓN al enviar producto {codigo}: {ex.Message}");
                        Utilidades.EscribirEnLog($"[SG1-POST-UNIT] EXCEPCIÓN al enviar producto {codigo}: {ex.Message}");
                    }
                    finally
                    {
                        ActualizarBase(statusCode, responseData, codigo);
                    }
                }
                catch (JsonException ex)
                {
                    Console.WriteLine($"[SG1-POST-UNIT] Error al parsear JSON: {ex.Message}");
                    Utilidades.EscribirEnLog($"[SG1-POST-UNIT] Error al parsear JSON: {ex.Message}");
                    Console.WriteLine($"[SG1-POST-UNIT] JSON recibido:\n{jsonString}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[SG1-POST-UNIT] Error general: {ex.Message}");
                    Utilidades.EscribirEnLog($"[SG1-POST-UNIT] Error general: {ex.Message}");
                }
            }

            if (esAcumuladorPropio && putAcumulador.Count > 0)
            {
                Console.WriteLine($"[SG1-POST-UNIT] Ejecutando PUT masivo (desde postSG1(string)) para {putAcumulador.Count} productos.");
                await putSG1(putAcumulador);
            }
        }

        private static (string codigo, List<List<Dictionary<string, string>>> estructura)
        ConvertirJsonAEstructura(string jsonString)
        {
            // Esperamos un JSON con campos: producto (string), estructura (array de arrays de objetos {campo, valor})
            var root = JObject.Parse(jsonString);

            string codigo = root["producto"]?.ToString()
                            ?? throw new InvalidOperationException("JSON sin 'producto'.");

            // estructura: JToken → List<List<Dictionary<string, string>>>
            var estructuraToken = root["estructura"]
                                  ?? throw new InvalidOperationException("JSON sin 'estructura'.");

            var estructura = estructuraToken.ToObject<List<List<Dictionary<string, string>>>>()
                            ?? throw new InvalidOperationException("No se pudo mapear 'estructura'.");

            return (codigo, estructura);
        }
        public static Dictionary<string, List<List<Dictionary<string, string>>>> jsonSG1_BOP()
        {
            string connectionString = "Server=10.0.0.82;Database=AgrometalBOP;User Id=sa;Password=Descar_2020;";

            // ✅ Elegir query según compatibilidad del XML/BD
            string queryElegida;
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                queryElegida = ObtenerConsultaSG1_BOP(conn);
                Utilidades.EscribirEnLog("SG1 -> Query elegida:\n" + queryElegida);
            }

            var converter = new SqlToJsonConverter(connectionString);
            var products = converter.ConvertSqlToHierarchicalJsons(queryElegida);

            var estructuras = new Dictionary<string, List<List<Dictionary<string, string>>>>();

            foreach (var product in products)
            {
                var listaHijos = new List<List<Dictionary<string, string>>>();

                foreach (var relacion in product.estructura)
                {
                    var codigo = relacion.First(c => c.campo == "codigo").valor;
                    var cantidad = relacion.First(c => c.campo == "cantidad").valor;

                    listaHijos.Add(new List<Dictionary<string, string>>
            {
                new Dictionary<string, string> { { "campo", "codigo" },   { "valor", codigo } },
                new Dictionary<string, string> { { "campo", "cantidad" }, { "valor", cantidad } }
            });
                }

                estructuras[product.producto] = listaHijos;
            }

            // ✅ Esto debería ejecutarse una sola vez, no dentro del foreach de debug
            AgregarFechaAServiciosTerceros(estructuras, DateTime.Now);

            // DEBUG opcional
            foreach (var parent in estructuras)
            {
                var jsonBody = new
                {
                    producto = parent.Key,
                    qtdBase = "1",
                    estructura = parent.Value
                };

                string jsonData = JsonConvert.SerializeObject(jsonBody, Formatting.Indented);
                Console.WriteLine("JSON generado:");
                Console.WriteLine(jsonData);
            }

            return estructuras;
        }


        public static async Task putSG1(Dictionary<string, List<List<Dictionary<string, string>>>> estructuras)
        {
            string url = "http://119.8.73.193:8086/rest/TCEstructura/Modificar/";
            string username = "USERREST";
            string password = "restagr";

            Console.WriteLine($"[SG1-PUT] Iniciando PUT masivo. Cantidad de productos: {estructuras.Count}");
            Utilidades.EscribirEnLog($"[SG1-PUT] Iniciando PUT masivo. Cantidad de productos: {estructuras.Count}");

            using (HttpClient client = new HttpClient())
            {
                var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
                client.DefaultRequestHeaders.Authorization =
                    new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

                foreach (var parent in estructuras)
                {
                    string producto = parent.Key;
                    int cantItems = parent.Value?.Count ?? 0;

                    Console.WriteLine($"[SG1-PUT] Preparando PUT para producto {producto} con {cantItems} ítems de estructura...");

                    var jsonBody = new
                    {
                        producto = parent.Key,
                        qtdBase = "1",
                        estructura = parent.Value
                    };

                    string jsonData = JsonConvert.SerializeObject(jsonBody, Formatting.Indented);
                    JObject obj = JObject.Parse(jsonData);
                    string? codigo = obj["producto"]?.ToString();

                    if (string.IsNullOrEmpty(codigo))
                    {
                        Console.WriteLine("[SG1-PUT] ERROR: No se pudo obtener el código del producto, se omite este envío.");
                        continue;
                    }

                    Console.WriteLine($"[SG1-PUT] JSON generado para producto {codigo} (primeros 800 caracteres):");
                    Console.WriteLine(jsonData);

                    int statusCode = 0;
                    string responseData = string.Empty;

                    try
                    {
                        var content = new StringContent(jsonData, Encoding.UTF8, "application/json");
                        HttpResponseMessage response = await client.PutAsync(url, content);

                        statusCode = (int)response.StatusCode;
                        responseData = await response.Content.ReadAsStringAsync();

                        if (response.IsSuccessStatusCode)
                        {
                            Console.WriteLine($"[SG1-PUT] PUT OK para producto {codigo}: {statusCode} - {responseData}");
                            Utilidades.EscribirEnLog($"[SG1-PUT] PUT OK para producto {codigo}: {statusCode} - {responseData}");
                        }
                        else
                        {
                            Console.WriteLine($"[SG1-PUT] PUT ERROR para producto {codigo}: {statusCode} {response.ReasonPhrase}. Contenido: {responseData}");
                            Utilidades.EscribirEnLog($"[SG1-PUT] PUT ERROR para producto {codigo}: {statusCode} {response.ReasonPhrase}. Contenido: {responseData}");
                        }

                        Console.WriteLine($"[SG1-PUT] Respuesta cruda para producto {codigo}: {responseData}, status {statusCode}");
                        Utilidades.EscribirEnLog($"[SG1-PUT] Respuesta cruda para producto {codigo}: {responseData}, status {statusCode}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[SG1-PUT] EXCEPCIÓN al enviar producto {producto}: {ex.Message}");
                        Utilidades.EscribirEnLog($"[SG1-PUT] EXCEPCIÓN al enviar producto {producto}: {ex.Message}");
                    }
                    finally
                    {
                        ActualizarBase(statusCode, responseData, codigo);
                    }
                }
            }

            Console.WriteLine("[SG1-PUT] PUT masivo finalizado.");
        }

        static void AgregarFechaAServiciosTerceros(
    Dictionary<string, List<List<Dictionary<string, string>>>> estructuras,
    DateTime fechaEnvio)
        {
            if (estructuras == null || estructuras.Count == 0) return;

            // Elegí el formato que necesite Protheus:
            // string fechaStr = fechaEnvio.ToString("yyyyMMdd");
            string fechaStr = fechaEnvio.ToString("yyyyMMdd");

            foreach (var kv in estructuras) // kv.Key = producto, kv.Value = estructura
            {
                var estructura = kv.Value;
                if (estructura == null) continue;

                foreach (var bloque in estructura) // cada bloque representa un item (PR, MP, servicio, etc.)
                {
                    if (bloque == null || bloque.Count == 0) continue;

                    // Buscar el "codigo" dentro del bloque
                    string? codigo = bloque
                        .FirstOrDefault(d =>
                            d != null &&
                            d.TryGetValue("campo", out var c) &&
                            string.Equals(c, "codigo", StringComparison.OrdinalIgnoreCase))
                        ?.GetValueOrDefault("valor");

                    if (!EsPRT(codigo)) continue;

                    // Agregar/actualizar "fecha"
                    var fechaDict = bloque.FirstOrDefault(d =>
                        d != null &&
                        d.TryGetValue("campo", out var c) &&
                        string.Equals(c, "fechaFin", StringComparison.OrdinalIgnoreCase));

                    if (fechaDict != null)
                    {
                        fechaDict["valor"] = fechaStr;
                    }
                    else
                    {
                        bloque.Add(new Dictionary<string, string>
                        {
                            ["campo"] = "fechaFin",
                            ["valor"] = fechaStr
                        });
                    }
                }
            }
        }

        static bool EsPRT(string? codigo)
        {
            if (string.IsNullOrWhiteSpace(codigo)) return false;

            // Regla: servicio de terceros => PR...T (T al final)
            return codigo.StartsWith("PR", StringComparison.OrdinalIgnoreCase) &&
                   codigo.EndsWith("T", StringComparison.OrdinalIgnoreCase);
        }


        public static Dictionary<string, List<List<Dictionary<string, string>>>> jsonSG1_MBOM()
        {
            //string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
            //                Integrated Security=True;TrustServerCertificate=True";

            string connectionString = "Server=10.0.0.82;Database=AgrometalBOP;User Id=sa;Password=Descar_2020;";

            Console.WriteLine("[SG1-JSON] Iniciando generación de estructuras SG1 desde SQL...");
            Utilidades.EscribirEnLog("[SG1-JSON] Iniciando generación de estructuras SG1 desde SQL...");

            string query = @"
        WITH CTE_Hierarchy AS (
    SELECT DISTINCT
        Occurrence.id_table,
        ProductRevision.name,
        Product.productId AS codigo,
        CAST(Occurrence.parentRef AS INT) AS parentRef,
        ProductRevision.revision,
        ProductRevision.subType,
        Occurrence.idXml
    FROM Occurrence
    LEFT JOIN ProductRevision 
           ON Occurrence.instancedRef = ProductRevision.id_Table
          AND Occurrence.idXml       = ProductRevision.idXml
    LEFT JOIN Product 
           ON ProductRevision.masterRef = Product.id_Table
          AND ProductRevision.idXml    = Product.idXml
    GROUP BY
        Occurrence.id_table, ProductRevision.name, Product.productId,
        Occurrence.parentRef, ProductRevision.revision,
        ProductRevision.subType, Occurrence.idXml
),
Base AS (
    SELECT DISTINCT
        COALESCE(Parent.name, '')              AS Nombre_Padre,
        COALESCE(CodFmt.CodigoPadre_Final, '') AS Process_codigo,
        Child.name                             AS Nombre_Hijo,
        CodFmt.CodigoHijo_Final                AS Codigo_Hijo,
        Child.subType                          AS Subtype_Hijo,
        Qty.CantidadFinal                      AS CantidadHijo_Total,
        Child.revision                         AS Revision,

        MIN('PA')                              AS Tipo,
        MIN('01')                              AS Deposito,
        MAX(
            CASE 
                WHEN uudUnidad.title = 'Agm4_Unidad'     THEN uudUnidad.value
                WHEN uudUnidad.title = 'Agm4_Kilogramos' THEN uudUnidad.value
                WHEN uudUnidad.title = 'Agm4_Litros'     THEN uudUnidad.value
                WHEN uudUnidad.title = 'Agm4_Metros'     THEN uudUnidad.value
                ELSE 'UN'
            END
        ) AS unMedida

    FROM CTE_Hierarchy Child
    LEFT JOIN CTE_Hierarchy Parent 
           ON Child.parentRef = Parent.id_table

    LEFT JOIN Form fUnidad
           ON Child.codigo = CASE
                                WHEN CHARINDEX('/', fUnidad.name) > 0 
                                    THEN LEFT(fUnidad.name, CHARINDEX('/', fUnidad.name) - 1)
                                ELSE fUnidad.name
                             END
    LEFT JOIN UserValue_UserData uudUnidad
           ON fUnidad.id_Table + 9 = uudUnidad.id_Father
          AND Child.idXml          = uudUnidad.idXml

    LEFT JOIN (
        SELECT
            oPadre.id_Table AS ParentOccurrenceId,
            pHijo.productId AS ChildCodigo,
            CASE 
                WHEN prHijo.subType = 'Agm4_MatPrimaRevision'
                    THEN SUM(TRY_CAST(uvud.value AS DECIMAL(18,6)))
                ELSE COUNT(DISTINCT oHijo.id_Table)
            END AS Cantidad
        FROM Product pHijo
        INNER JOIN ProductRevision prHijo 
                ON pHijo.id_Table = prHijo.masterRef
        LEFT JOIN Occurrence oHijo       
               ON oHijo.instancedRef = prHijo.id_Table
        LEFT JOIN UserValue_UserData uvud 
               ON uvud.id_Father = oHijo.id_Table + 2 
              AND uvud.title    = 'Quantity'
        LEFT JOIN Occurrence oPadre 
               ON oHijo.parentRef = oPadre.id_Table
        GROUP BY oPadre.id_Table, pHijo.productId, prHijo.subType
    ) sq3
        ON sq3.ParentOccurrenceId = Parent.id_table
       AND sq3.ChildCodigo        = Child.codigo

    OUTER APPLY (
        SELECT CAST(
            CASE 
                WHEN Parent.id_table IS NULL THEN 1
                ELSE ISNULL(sq3.Cantidad, 1)
            END
        AS DECIMAL(18,2)) AS CantidadFinal
    ) AS Qty

    OUTER APPLY (
        SELECT
            CASE 
                WHEN Parent.codigo IS NULL THEN NULL
                ELSE
                    CASE 
                        WHEN LEFT(Parent.codigo, 2) = 'M-' THEN RIGHT(Parent.codigo, LEN(Parent.codigo) - 2)
                        WHEN LEFT(Parent.codigo, 1) = 'M'  THEN RIGHT(Parent.codigo, LEN(Parent.codigo) - 1)
                        WHEN LEFT(Parent.codigo, 1) = 'E'  THEN RIGHT(Parent.codigo, LEN(Parent.codigo) - 1)
                        WHEN RIGHT(Parent.codigo, 3) = '-FV' THEN LEFT(Parent.codigo, LEN(Parent.codigo) - 3)
                        ELSE Parent.codigo
                    END
            END AS CodigoPadre_SB1,

            CASE 
                WHEN Child.codigo IS NULL THEN NULL
                ELSE
                    CASE 
                        WHEN LEFT(Child.codigo, 2) = 'M-' THEN RIGHT(Child.codigo, LEN(Child.codigo) - 2)
                        WHEN LEFT(Child.codigo, 1) = 'M'  THEN RIGHT(Child.codigo, LEN(Child.codigo) - 1)
                        WHEN LEFT(Child.codigo, 1) = 'E'  THEN RIGHT(Child.codigo, LEN(Child.codigo) - 1)
                        WHEN RIGHT(Child.codigo, 3) = '-FV' THEN LEFT(Child.codigo, LEN(Child.codigo) - 3)
                        ELSE Child.codigo
                    END
            END AS CodigoHijo_SB1
    ) CodSB1

    OUTER APPLY (
        SELECT
            CASE 
                WHEN Parent.codigo IS NULL THEN NULL
                ELSE
                    CASE 
                        WHEN LEFT(CodSB1.CodigoPadre_SB1, 1) = 'E'
                            THEN SUBSTRING(CodSB1.CodigoPadre_SB1, 2, LEN(CodSB1.CodigoPadre_SB1) - 1)
                        ELSE CodSB1.CodigoPadre_SB1
                    END
            END AS CodigoPadre_SinE,

            CASE 
                WHEN Parent.codigo IS NULL THEN CodSB1.CodigoHijo_SB1
                ELSE
                    CASE 
                        WHEN LEFT(CodSB1.CodigoHijo_SB1, 1) = 'E'
                            THEN SUBSTRING(CodSB1.CodigoHijo_SB1, 2, LEN(CodSB1.CodigoHijo_SB1) - 1)
                        ELSE CodSB1.CodigoHijo_SB1
                    END
            END AS CodigoHijo_SinE
    ) CodSinE

    OUTER APPLY (
        SELECT
            CASE 
                WHEN CodSinE.CodigoPadre_SinE IS NULL THEN NULL
                ELSE
                    CASE 
                        WHEN RIGHT(CodSinE.CodigoPadre_SinE, 2) LIKE '-[0-9]'
                            THEN LEFT(CodSinE.CodigoPadre_SinE, LEN(CodSinE.CodigoPadre_SinE) - 2)
                        ELSE CodSinE.CodigoPadre_SinE
                    END
            END AS CodigoPadre_Final,

            CASE 
                WHEN Parent.codigo IS NULL THEN CodSinE.CodigoHijo_SinE
                ELSE
                    CASE 
                        WHEN RIGHT(CodSinE.CodigoHijo_SinE, 2) LIKE '-[0-9]'
                            THEN LEFT(CodSinE.CodigoHijo_SinE, LEN(CodSinE.CodigoHijo_SinE) - 2)
                        ELSE CodSinE.CodigoHijo_SinE
                    END
            END AS CodigoHijo_Final
    ) CodFmt

    WHERE
        Child.subType IN (
            'Agm4_ConGeneralRevision',
            'Agm4_MatPrimaRevision',
            'Agm4_PiezaRevision',
            'Agm4_RepCompradoRevision',
            'Agm4_SubConRevision',
            'Agm4_sub_mBOM_ERevision'
        )
    GROUP BY
        Parent.name,
        CodFmt.CodigoPadre_Final,
        Child.name,
        CodFmt.CodigoHijo_Final,
        Qty.CantidadFinal,
        Child.revision,
        Child.subType
)
SELECT
    b.*,
    CASE
        WHEN b.Subtype_Hijo = 'Agm4_SubConRevision'
             AND b.Nombre_Hijo NOT LIKE '%INT.%'
            THEN 'S'
        WHEN b.Subtype_Hijo = 'Agm4_sub_mBOM_ERevision'
             AND LEN(ISNULL(b.Codigo_Hijo,'')) = 6
            THEN 'S'
		WHEN b.Subtype_Hijo = 'Agm4_ConGeneralRevision'
             AND b.Nombre_Hijo NOT LIKE '%INT.%'
            THEN 'S'
        ELSE 'N'
    END AS Es_Fantasma
FROM Base b
--WHERE Process_codigo = '066650'
ORDER BY b.Process_codigo;
        ";

            var estructuras = new Dictionary<string, List<List<Dictionary<string, string>>>>();

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    Console.WriteLine("[SG1-JSON] Conexión a SQL abierta.");

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 5000;
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            var dataByParent = new Dictionary<string, List<DataModel>>();
                            int filasLeidas = 0;

                            while (reader.Read())
                            {
                                filasLeidas++;

                                string parentName = reader["Nombre_Padre"]?.ToString() ?? string.Empty;
                                string rawParentCod = reader["Process_codigo"]?.ToString();
                                string childName = reader["Nombre_Hijo"]?.ToString() ?? string.Empty;
                                string childCodigo = reader["Codigo_Hijo"]?.ToString() ?? string.Empty;
                                string cantidadHijo = reader["CantidadHijo_Total"]?.ToString().Replace(',', '.') ?? string.Empty;

                                // ⚠️ AQUÍ VIENE EL CAMBIO IMPORTANTE
                                string parentCodigo = rawParentCod;
                                bool esFilaRaiz = false;

                                if (string.IsNullOrWhiteSpace(parentCodigo))
                                {
                                    if (!string.IsNullOrWhiteSpace(childCodigo))
                                    {
                                        // Esta fila representa al PADRE SUPREMO:
                                        // usamos el código del hijo como código de padre lógico
                                        parentCodigo = childCodigo;
                                        esFilaRaiz = true;
                                        Console.WriteLine($"[SG1-JSON] Fila raíz detectada → ParentCodigo={parentCodigo}, ChildCodigo={childCodigo}");
                                        Utilidades.EscribirEnLog($"[SG1-JSON] Fila raíz detectada → ParentCodigo={parentCodigo}, ChildCodigo={childCodigo}");
                                    }
                                    else
                                    {
                                        // Caso realmente inválido: sin padre ni hijo
                                        Console.WriteLine("[SG1-JSON] WARNING: fila sin Process_codigo ni Codigo_Hijo. Se omite.");
                                        continue;
                                    }
                                }

                                var model = new DataModel
                                {
                                    ParentName = parentName,
                                    ParentCodigo = parentCodigo,
                                    ChildName = childName,
                                    ChildCodigo = childCodigo,
                                    CantidadHijo = cantidadHijo,
                                    //Variante = ...
                                };

                                // ✅ SIEMPRE poblar la tabla SG1 local (incluye al padre supremo)
                                poblarBaseSG1(parentName, parentCodigo, childName, childCodigo, cantidadHijo);

                                if (!dataByParent.ContainsKey(model.ParentCodigo))
                                    dataByParent[model.ParentCodigo] = new List<DataModel>();

                                dataByParent[model.ParentCodigo].Add(model);
                            }

                            Console.WriteLine($"[SG1-JSON] SQL procesado. Filas leídas: {filasLeidas}. Padres distintos (lógicos): {dataByParent.Count}");
                            Utilidades.EscribirEnLog($"[SG1-JSON] SQL procesado. Filas leídas: {filasLeidas}. Padres distintos (lógicos): {dataByParent.Count}");

                            // ====== ARMADO DE ESTRUCTURAS PARA CADA PADRE ======
                            foreach (var parentGroup in dataByParent)
                            {
                                string parentCodigo = parentGroup.Key;
                                List<DataModel> children = parentGroup.Value;

                                if (!estructuras.ContainsKey(parentCodigo))
                                    estructuras[parentCodigo] = new List<List<Dictionary<string, string>>>();

                                var allConditions = new Dictionary<string, List<string>>();

               

                                // ✅ ACÁ: cuando ya está todo armado (después del foreach dataByParent)
                                string json = JsonConvert.SerializeObject(estructuras, Formatting.Indented);

                                Console.WriteLine("[JSON MBOM]");
                                Console.WriteLine(json);

                                // (opcional) a archivo para no saturar consola
                                File.WriteAllText("mbom_debug.json", json);
                                Console.WriteLine("[MBOM] Dump a mbom_debug.json");

                                // Recolección de variantes (si la usás)
                                foreach (var child in children)
                                {
                                    if (!string.IsNullOrEmpty(child.Variante))
                                    {
                                        try
                                        {
                                            var conditions = ExtractAllConditions(child.Variante);
                                            foreach (var condition in conditions)
                                            {
                                                if (condition.Key == null)
                                                {
                                                    Console.WriteLine($"WARNING: Null key found in conditions for Variante: {child.Variante}");
                                                    continue;
                                                }

                                                if (!allConditions.ContainsKey(condition.Key))
                                                    allConditions[condition.Key] = new List<string>();

                                                if (!allConditions[condition.Key].Contains(condition.Value))
                                                    allConditions[condition.Key].Add(condition.Value);
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            Console.WriteLine($"Error processing Variante '{child.Variante}': {ex.Message}");
                                        }
                                    }
                                }

                                var configCounter = new Dictionary<string, int>();

                                foreach (var child in children)
                                {
                                    // 🚫 NO generar componente autoreferenciado (producto supremo dentro de su propia estructura)
                                    if (!string.IsNullOrEmpty(child.ChildCodigo) &&
                                        child.ChildCodigo == parentCodigo)
                                    {
                                        Console.WriteLine($"[SG1-JSON] INFO: se omite línea autoreferenciada {child.ChildCodigo} en estructura de {parentCodigo}");
                                        continue;
                                    }

                                    var childStructure = new List<Dictionary<string, string>>
                                    {
                                        new() { { "campo", "codigo"   }, { "valor", child.ChildCodigo } },
                                        new() { { "campo", "cantidad" }, { "valor", child.CantidadHijo } }
                                    };

                                    // Sin variante → solo código + cantidad
                                    if (string.IsNullOrEmpty(child.Variante))
                                    {
                                        estructuras[parentCodigo].Add(childStructure);
                                        continue;
                                    }

                                    Dictionary<string, string> conditions;
                                    try
                                    {
                                        conditions = ExtractAllConditions(child.Variante);
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"Error extracting conditions from '{child.Variante}': {ex.Message}");
                                        estructuras[parentCodigo].Add(childStructure);
                                        continue;
                                    }

                                    string grupoOpc = "001";
                                    string prefijoOpcional = null;

                                    // (toda la lógica de SSE/SSH/SSM/FSE/... igual que antes)
                                    if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SOLO SEMILLA") &&
                                        ContainsCondition(conditions, "SEMILLA-", "ELECTRICA"))
                                    {
                                        prefijoOpcional = "SSE";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SOLO SEMILLA") &&
                                             ContainsCondition(conditions, "SEMILLA-", "HIDRAULICA"))
                                    {
                                        prefijoOpcional = "SSH";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SOLO SEMILLA") &&
                                             ContainsCondition(conditions, "SEMILLA-", "MECANICA"))
                                    {
                                        prefijoOpcional = "SSM";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SEMILLA + SIMPLE FERTILIZACION") &&
                                             ContainsCondition(conditions, "FERTILIZACION-SIMPLE", "ELECTRICA"))
                                    {
                                        prefijoOpcional = "FSE";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SEMILLA + SIMPLE FERTILIZACION") &&
                                             ContainsCondition(conditions, "FERTILIZACION-SIMPLE", "HIDRAULICA"))
                                    {
                                        prefijoOpcional = "FSH";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SEMILLA + SIMPLE FERTILIZACION") &&
                                             ContainsCondition(conditions, "FERTILIZACION-SIMPLE", "MECANICA"))
                                    {
                                        prefijoOpcional = "FSM";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SEMILLA + DOBLE FERTILIZACION") &&
                                             ContainsCondition(conditions, "FERTILIZACION-DOBLE", "HIDRAULICA"))
                                    {
                                        prefijoOpcional = "FDH";
                                    }
                                    else if (ContainsCondition(conditions, "CONFIGURACION DE USO", "SEMILLA + DOBLE FERTILIZACION") &&
                                             ContainsCondition(conditions, "FERTILIZACION-DOBLE", "MECANICA"))
                                    {
                                        prefijoOpcional = "FDM";
                                    }
                                    else
                                    {
                                        prefijoOpcional = "";
                                        Console.WriteLine(string.Join("", conditions.Keys));
                                    }

                                    if (grupoOpc != null &&
                                        !string.IsNullOrEmpty(prefijoOpcional) &&
                                        prefijoOpcional.Length > 1)
                                    {
                                        if (!configCounter.ContainsKey(prefijoOpcional))
                                            configCounter[prefijoOpcional] = 1;

                                        childStructure.Add(new Dictionary<string, string>
                                        {
                                            { "campo", "grupo_opc" },
                                            { "valor", grupoOpc }
                                        });

                                        childStructure.Add(new Dictionary<string, string>
                                        {
                                            { "campo", "opcional" },
                                            { "valor", prefijoOpcional }
                                        });
                                    }

                                    estructuras[parentCodigo].Add(childStructure);
                                }

                                Console.WriteLine($"[SG1-JSON] Padre {parentCodigo}: hijos en estructura = {estructuras[parentCodigo].Count}");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SG1-JSON] ERROR al consultar la base: {ex.Message}");
                Utilidades.EscribirEnLog($"[SG1-JSON] ERROR al consultar la base: {ex.Message}");
                Console.WriteLine($"[SG1-JSON] Stack trace: {ex.StackTrace}");
                Utilidades.EscribirEnLog($"[SG1-JSON] Stack trace: {ex.StackTrace}");
            }

            Console.WriteLine($"[SG1-JSON] Estructuras generadas para {estructuras.Count} productos.");
            Utilidades.EscribirEnLog($"[SG1-JSON] Estructuras generadas para {estructuras.Count} productos.");

            return estructuras;
        }
        

        public static void poblarBaseSG1(string Nombre_Padre, string Codigo_Padre, string Nombre_Hijo, string Codigo_Hijo, string CantidadHijo)
        {
            //string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
            //                    Integrated Security=True;TrustServerCertificate=True";

            string connectionString = "Server=10.0.0.82;Database=AgrometalBOP;User Id=sa;Password=Descar_2020;";
            string query = "INSERT INTO SG1 VALUES (@Nombre_Padre, @Codigo_Padre, @Nombre_Hijo, @Codigo_Hijo, @CantidadHijo, NULL, NULL)";
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@Nombre_Padre", Nombre_Padre);
                        command.Parameters.AddWithValue("@Codigo_Padre", Codigo_Padre);
                        command.Parameters.AddWithValue("@Nombre_Hijo", Nombre_Hijo);
                        command.Parameters.AddWithValue("@Codigo_Hijo", Codigo_Hijo);
                        command.Parameters.AddWithValue("@CantidadHijo", CantidadHijo);
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SG1-DB] ERROR en poblarBaseSG1 para padre {Codigo_Padre}, hijo {Codigo_Hijo}: {ex.Message}");
            }
        }

        public static void ActualizarBase(int estado, string mensaje, string codigo)
        {
            //string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
            //                    Integrated Security=True;TrustServerCertificate=True";

            string connectionString = "Server=10.0.0.82;Database=AgrometalBOP;User Id=sa;Password=Descar_2020;";
            string query = @"UPDATE SG1
                     SET estado = @estado, mensaje = @mensaje
                     WHERE Codigo_Padre = @codigo";

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@estado", estado);
                        command.Parameters.AddWithValue("@mensaje", mensaje ?? string.Empty);
                        command.Parameters.AddWithValue("@codigo", codigo);
                        int rows = command.ExecuteNonQuery();
                        Console.WriteLine($"[SG1-DB] ActualizarBase: estado={estado} codigo={codigo} filas_afectadas={rows}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SG1-DB] ERROR en ActualizarBase para codigo {codigo}: {ex.Message}");
            }
        }

        private static bool ContainsCondition(Dictionary<string, string> conditions, string key, string value)
        {
            return conditions.ContainsKey(key) && conditions[key] == value;
        }

        private static Dictionary<string, string> ExtractAllConditions(string varianteText)
        {
            var conditions = new Dictionary<string, string>();

            if (string.IsNullOrEmpty(varianteText))
                return conditions;

            // Split by & to get different conditions
            string[] parts = varianteText.Split(new[] { " & " }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var part in parts)
            {
                try
                {
                    // Look for pattern "[Teamcenter]KEY = VALUE" or "[Teamcenter]''KEY'' = ''VALUE''"
                    string pattern = part.Trim();

                    // Extract the part after [Teamcenter]
                    int teamcenterPos = pattern.IndexOf("[Teamcenter]");
                    if (teamcenterPos >= 0)
                    {
                        string keyValuePart = pattern.Substring(teamcenterPos + "[Teamcenter]".Length).Trim();

                        // Handle case of double single quotes
                        if (keyValuePart.Contains("''"))
                        {
                            // Split by equal sign
                            string[] keyValueSplit = keyValuePart.Split('=');
                            if (keyValueSplit.Length == 2)
                            {
                                // Extract key and value, removing double single quotes
                                string keyWithQuotes = keyValueSplit[0].Trim();
                                string valueWithQuotes = keyValueSplit[1].Trim();

                                // Remove double single quotes
                                string key = keyWithQuotes.Replace("''", "").Trim();
                                string value = valueWithQuotes.Replace("''", "").Trim();

                                // Check for null key
                                if (!string.IsNullOrEmpty(key))
                                {
                                    conditions[key] = value ?? string.Empty;
                                }
                                else
                                {
                                    Console.WriteLine($"Warning: Null or empty key found in '{keyValuePart}'");
                                }
                            }
                        }
                        else
                        {
                            // No quotes, look for equal sign
                            string[] keyValue = keyValuePart.Split('=');
                            if (keyValue.Length == 2)
                            {
                                string key = keyValue[0].Trim();
                                string value = keyValue[1].Trim();

                                // Check for null key
                                if (!string.IsNullOrEmpty(key))
                                {
                                    conditions[key] = value ?? string.Empty;
                                }
                                else
                                {
                                    Console.WriteLine($"Warning: Null or empty key found in '{keyValuePart}'");
                                }
                            }
                        }
                    }
                    else
                    {
                        // Try to extract from other formats like [PREFIX]KEY = VALUE
                        Match match = Regex.Match(pattern, @"\[([^\]]+)\]([^=]+)=(.+)");
                        if (match.Success)
                        {
                            string prefix = match.Groups[1].Value.Trim();
                            string key = match.Groups[2].Value.Trim();
                            string value = match.Groups[3].Value.Trim();

                            // Check for null key
                            if (!string.IsNullOrEmpty(key))
                            {
                                conditions[key] = value ?? string.Empty;
                            }
                            else
                            {
                                Console.WriteLine($"Warning: Null or empty key found in '{pattern}'");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing condition part '{part}': {ex.Message}");
                    // Continue with next condition in case of error
                }
            }
            return conditions;
        }
    }
}
