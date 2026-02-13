using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

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
        private const string SG1_POST_URL = "http://119.8.73.193:8096/rest/TCEstructura/Incluir/";
        private const string SG1_PUT_URL = "http://119.8.73.193:8096/rest/TCEstructura/Modificar/";
        private const string SG1_DELETE_URL = "http://119.8.73.193:8096/rest/TCEstructura/Eliminar/";

        private const string USERNAME = "USERREST";
        private const string PASSWORD = "restagr";

        private static readonly TimeSpan RetryDelay = TimeSpan.FromMinutes(5);

        private static readonly HttpClient _clientSG1 = CreateHttpClient();

        private static async Task<bool> EliminarEstructurasBopAsync(IEnumerable<string> codigos)
        {
            bool okGlobal = true;

            foreach (var codigo in codigos.Where(c => !string.IsNullOrWhiteSpace(c)))
            {
                var del = await EliminarEstructuraSG1Async(codigo);

                if (!del.ok)
                {
                    okGlobal = false;
                    Utilidades.EscribirEnLog($"[SG1-RESET] DELETE falló para {codigo}. status={del.statusCode}. resp={del.responseBody}");
                }
                else
                {
                    Utilidades.EscribirEnLog($"[SG1-RESET] DELETE OK para {codigo}. status={del.statusCode}");
                }
            }

            return okGlobal;
        }

        private static HttpClient CreateHttpClient()
        {
            var client = new HttpClient
            {
                Timeout = TimeSpan.FromMinutes(3) // ajustá si Protheus tarda más
            };

            var credentials = Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes($"{USERNAME}:{PASSWORD}"));
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);

            return client;
        }

        private static bool IsRetryableStatus(HttpStatusCode code)
            => (int)code >= 500 && (int)code <= 599;

        private static bool IsRetryableException(Exception ex)
            => ex is HttpRequestException
            || ex is TaskCanceledException
            || ex is IOException;
        // Productos que en la query original venían sin Codigo_Padre
        private static readonly HashSet<string> ProductosSinCodigoPadre =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private static async Task<(int statusCode, HttpStatusCode httpStatus, string responseBody)>
PostSG1RawAsync(string codigo, string jsonData)
        {
            int intento = 0;

            while (true)
            {
                intento++;

                try
                {
                    await ProtheusHealth.WaitUntilActiveAsync("SG1-POST-REC", RetryDelay);

                    using var content = new StringContent(jsonData, Encoding.UTF8, "application/json");
                    using HttpResponseMessage response = await _clientSG1.PostAsync(SG1_POST_URL, content);

                    int statusCode = (int)response.StatusCode;
                    string responseData = await response.Content.ReadAsStringAsync();

                    LogErrorProtheusIfAny("SG1", "POST", codigo, responseData);

                    // 5xx => retry infinito
                    if (IsRetryableStatus(response.StatusCode))
                    {
                        Console.WriteLine($"[SG1-POST-REC] {statusCode} para {codigo}. Reintento infinito en 5 minutos. Intento #{intento}");
                        Utilidades.EscribirEnLog($"[SG1-POST-REC] {statusCode} para {codigo}. Reintento infinito en 5 minutos. Intento #{intento}. Resp: {responseData}");
                        await Task.Delay(RetryDelay);
                        continue;
                    }

                    // 2xx o 4xx (incluye 409) => devolvemos al caller para decidir
                    return (statusCode, response.StatusCode, responseData);
                }
                catch (Exception ex) when (IsRetryableException(ex))
                {
                    Console.WriteLine($"[SG1-POST-REC] EXCEPCIÓN transitoria para {codigo}: {ex.Message}. Reintento infinito en 5 minutos. Intento #{intento}");
                    Utilidades.EscribirEnLog($"[SG1-POST-REC] EXCEPCIÓN transitoria para {codigo}: {ex.Message}. Reintento infinito en 5 minutos. Intento #{intento}");
                    await Task.Delay(RetryDelay);
                    continue;
                }
            }
        }

        private sealed class WsError
        {
            public int? errorCode { get; set; }
            public string errorMessage { get; set; }
        }
        private static bool IsRecursividadError(string responseBody)
        {
            if (string.IsNullOrWhiteSpace(responseBody))
                return false;

            // Caso ideal: viene JSON con errorCode + errorMessage
            if (TryParseWsError(responseBody, out var code, out var msg))
            {
                if (code == 6 && ContainsRecursiv(msg))
                    return true;
            }

            // Fallback: por si el body no es JSON (o viene distinto)
            return ContainsRecursiv(responseBody);
        }

        private static bool ContainsRecursiv(string? text)
        {
            if (string.IsNullOrWhiteSpace(text))
                return false;

            // Matchea "recursividade", "recursividade na estrutura", etc.
            return text.IndexOf("recursiv", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static async Task<(bool ok, int statusCode, string responseBody)> EliminarEstructuraSG1Async(string codigo)
        {
            int intento = 0;

            while (true)
            {
                intento++;

                try
                {
                    await ProtheusHealth.WaitUntilActiveAsync("SG1-DELETE", RetryDelay);

                    var payload = JsonConvert.SerializeObject(new { producto = codigo }, Formatting.None);

                    using var req = new HttpRequestMessage(HttpMethod.Delete, SG1_DELETE_URL)
                    {
                        Content = new StringContent(payload, Encoding.UTF8, "application/json")
                    };

                    using HttpResponseMessage resp = await _clientSG1.SendAsync(req);

                    int status = (int)resp.StatusCode;
                    string body = await resp.Content.ReadAsStringAsync();
                    LogErrorProtheusIfAny("SG1", "DELETE", codigo, body);

                    if (IsRetryableStatus(resp.StatusCode))
                    {
                        Console.WriteLine($"[SG1-DELETE] {status} para {codigo}. Reintento en 5 minutos. Intento #{intento}");
                        Utilidades.EscribirEnLog($"[SG1-DELETE] {status} para {codigo}. Reintento en 5 minutos. Intento #{intento}. Resp: {body}");
                        await Task.Delay(RetryDelay);
                        continue;
                    }

                    if (resp.IsSuccessStatusCode)
                    {
                        Console.WriteLine($"[SG1-DELETE] OK para {codigo}: {status} - {body}");
                        Utilidades.EscribirEnLog($"[SG1-DELETE] OK para {codigo}: {status} - {body}");
                        return (true, status, body);
                    }

                    // 4xx u otros => no retry (corte controlado)
                    Console.WriteLine($"[SG1-DELETE] ERROR NO-RETRY para {codigo}: {status} {resp.ReasonPhrase}. Contenido: {body}");
                    Utilidades.EscribirEnLog($"[SG1-DELETE] ERROR NO-RETRY para {codigo}: {status} {resp.ReasonPhrase}. Contenido: {body}");
                    return (false, status, body);
                }
                catch (Exception ex) when (IsRetryableException(ex))
                {
                    Console.WriteLine($"[SG1-DELETE] EXCEPCIÓN transitoria para {codigo}: {ex.Message}. Reintento en 5 minutos. Intento #{intento}");
                    Utilidades.EscribirEnLog($"[SG1-DELETE] EXCEPCIÓN transitoria para {codigo}: {ex.Message}. Reintento en 5 minutos. Intento #{intento}");
                    await Task.Delay(RetryDelay);
                    continue;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[SG1-DELETE] EXCEPCIÓN NO transitoria para {codigo}: {ex}");
                    Utilidades.EscribirEnLog($"[SG1-DELETE] EXCEPCIÓN NO transitoria para {codigo}: {ex}");
                    return (false, 0, ex.ToString());
                }
            }
        }

        private static bool TryParseWsError(string body, out int? errorCode, out string errorMessage)
        {
            errorCode = null;
            errorMessage = null;

            if (string.IsNullOrWhiteSpace(body))
                return false;

            try
            {
                // Caso típico: {"errorCode":10,"errorMessage":"..."}
                var err = JsonConvert.DeserializeObject<WsError>(body);
                if (err?.errorCode != null)
                {
                    errorCode = err.errorCode;
                    errorMessage = err.errorMessage;
                    return true;
                }

                // Fallback: JObject (por si viene otra estructura)
                var j = JObject.Parse(body);
                var codeToken = j["errorCode"];
                if (codeToken != null && int.TryParse(codeToken.ToString(), out var code))
                {
                    errorCode = code;
                    errorMessage = j["errorMessage"]?.ToString();
                    return true;
                }
            }
            catch
            {
                // body no es JSON o no tiene el schema esperado
            }

            return false;
        }

        private static void LogErrorProtheusIfAny(string tabla, string metodo, string producto, string responseBody)
        {
            if (TryParseWsError(responseBody, out var code, out var msg))
            {
                Utilidades.EscribirErrorProtheus(tabla, metodo, producto, code, msg);
            }
        }

        public Tabla_SG1()
        {
            _httpClient = new HttpClient();
        }
        // ✅ Query original (la que ya tenés) — SIN CAMBIOS
        private const string consultaSG1_BOP_ConWorkArea = @"
WITH FirstProcessName AS (
    SELECT TOP (1)
        CASE
            WHEN LEN(p.catalogueId) != 8 THEN RIGHT(p.catalogueId, LEN(p.catalogueId) - 2)
            ELSE RIGHT(p.catalogueId, 6)
        END AS first_process_name
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
                THEN ISNULL(qty_pick.QtyValue, 0)
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
        ON (
            o.parentRef = po.id_Table
            OR o.parentRef = po_op.id_Table
        )
       AND ISNULL(o.subType,'') <> 'METool'
    LEFT JOIN ProductRevision prod_rev
        ON prod_rev.id_Table = o.instancedRef
    LEFT JOIN Product prod
        ON prod.id_Table = prod_rev.masterRef
    OUTER APPLY (
        SELECT TOP (1) q.QtyValue
        FROM (
            SELECT
                TRY_CONVERT(FLOAT, REPLACE(NULLIF(LTRIM(RTRIM(uvO.value)), ''), ',', '.')) AS QtyValue,
                1 AS prio
            FROM UserValue_Occurrence uvO
            WHERE uvO.id_Father = o.id_Table
              AND uvO.title IN ('Quantity','quantity')

            UNION ALL

            SELECT
                TRY_CONVERT(FLOAT, REPLACE(NULLIF(LTRIM(RTRIM(uvUD.value)), ''), ',', '.')) AS QtyValue,
                2 AS prio
            FROM UserData ud
            JOIN UserValue_UserData uvUD
              ON uvUD.id_Father = ud.id_Table
             AND uvUD.title IN ('Quantity','quantity')
            WHERE ud.id_Father = o.id_Table

            UNION ALL

            SELECT
                TRY_CONVERT(FLOAT, REPLACE(NULLIF(LTRIM(RTRIM(uvUD2.value)), ''), ',', '.')) AS QtyValue,
                3 AS prio
            FROM UserValue_UserData uvUD2
            WHERE uvUD2.id_Father = o.id_Table + 2
              AND uvUD2.title IN ('Quantity','quantity')

        ) q
        WHERE q.QtyValue IS NOT NULL
        ORDER BY q.prio
    ) qty_pick
    WHERE
        prod.productId IS NOT NULL
        AND NOT (
            prod_rev.subType IN ('Mfg0MEFactoryToolRevision', 'Mfg0MEResourceRevision')
            OR
            (
                prod_rev.subType IN ('Agm4_MatPrimaRevision','Agm4_RepCompradoRevision')
                AND ISNULL(qty_pick.QtyValue, 0) <= 0
            )
        )
)
SELECT
    fpn.first_process_name AS Process_codigo,
    prcodet.PR_Codigo_Protheus AS PR_Codigo,
    vc.Codigo AS Codigo,
    SUM(ISNULL(vc.Qty, 0)) AS Cantidad,
    vc.subType AS subType,
    seq_pick.SeqValue AS num_busqueda,
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
    FROM (
        SELECT po.id_Table AS ParentRef
        UNION ALL
        SELECT po_op2.id_Table
        FROM ProcessOccurrence po_op2
        WHERE po_op2.parentRef = po.id_Table
          AND po_op2.idXml = po.idXml
    ) prf
    JOIN Occurrence owa
      ON owa.parentRef = prf.ParentRef
     AND owa.idXml = po.idXml
     AND owa.subType IN ('MEWorkarea','MEWorkArea')
    JOIN WorkAreaRevision war2
      ON war2.id_Table = owa.instancedRef
     AND war2.idXml = owa.idXml
    JOIN WorkArea wa2
      ON wa2.id_Table = war2.masterRef
     AND wa2.idXml = war2.idXml
    ORDER BY
        CASE WHEN wa2.name LIKE '%TERCEROS%' THEN 0 ELSE 1 END,
        owa.id_Table
) wa_pick

OUTER APPLY (
    SELECT TOP (1) s.SeqValue
    FROM (
        SELECT
            NULLIF(LTRIM(RTRIM(uvpo.value)), '') AS SeqValue,
            1 AS prio
        FROM UserValue_ProcessOccurrence uvpo
        WHERE uvpo.id_Father = po.id_Table
          AND uvpo.title IN ('SequenceNumber','sequencenumber','sequenceNumber')

        UNION ALL

        SELECT
            NULLIF(LTRIM(RTRIM(uvpo2.value)), '') AS SeqValue,
            2 AS prio
        FROM UserValue_ProcessOccurrence uvpo2
        WHERE uvpo2.id_Father = po.id_Table
          AND uvpo2.title LIKE '%Sequence%'

        UNION ALL

        SELECT
            NULLIF(LTRIM(RTRIM(uv.value)), '') AS SeqValue,
            3 AS prio
        FROM UserData ud
        JOIN UserValue_UserData uv
          ON uv.id_Father = ud.id_Table
         AND uv.title IN ('SequenceNumber','sequencenumber')
        WHERE ud.id_Father = po.id_Table

        UNION ALL

        SELECT
            NULLIF(LTRIM(RTRIM(uv2.value)), '') AS SeqValue,
            4 AS prio
        FROM UserValue_UserData uv2
        WHERE uv2.id_Father = po.id_Table + 2
          AND uv2.title IN ('SequenceNumber','sequencenumber')

        UNION ALL

        SELECT
            NULLIF(LTRIM(RTRIM(uv3.value)), '') AS SeqValue,
            5 AS prio
        FROM UserValue_UserData uv3
        WHERE uv3.id_Father = po.id_Table
          AND uv3.title IN ('SequenceNumber','sequencenumber')
    ) s
    WHERE s.SeqValue IS NOT NULL
    ORDER BY s.prio
) seq_pick

CROSS APPLY (
    SELECT
        CASE
            WHEN LEFT(p.catalogueId, 1) IN ('M','E')
                THEN RIGHT(p.catalogueId, LEN(p.catalogueId) - 1)
            WHEN RIGHT(p.catalogueId, 3) = '-FV'
                THEN LEFT(p.catalogueId, LEN(p.catalogueId) - 3)
            WHEN LEFT(p.catalogueId, 2) = 'P-'
                THEN RIGHT(p.catalogueId, LEN(p.catalogueId) - 2)
            ELSE p.catalogueId
        END AS PR_Codigo_Norm
) prcode

CROSS APPLY (
    SELECT
        CASE
            WHEN prcode.PR_Codigo_Norm IS NOT NULL
             AND LEFT(prcode.PR_Codigo_Norm, 2) = 'PR'
             AND RIGHT(prcode.PR_Codigo_Norm, 1) <> 'T'
             AND (
                    wa_pick.WorkArea_CatalogueId = '000465'
                    OR UPPER(LTRIM(RTRIM(wa_pick.WorkArea_Nombre))) = 'TERCEROS'
                    OR UPPER(wa_pick.WorkArea_Nombre) LIKE '%TERCEROS%'
                 )
                THEN prcode.PR_Codigo_Norm + 'T'
            ELSE prcode.PR_Codigo_Norm
        END AS PR_Codigo_Protheus
) prcodet

LEFT JOIN ValidChildren vc
    ON vc.PR_OccId = po.id_Table
CROSS JOIN FirstProcessName fpn
WHERE
    fpn.first_process_name <> prcode.PR_Codigo_Norm
GROUP BY
    fpn.first_process_name,
    p.catalogueId,
    prcodet.PR_Codigo_Protheus,
    vc.Codigo,
    vc.subType,
    seq_pick.SeqValue,
    wa_pick.WorkArea_CatalogueId,
    wa_pick.WorkArea_Nombre,
    wa_pick.WorkArea_Revision
ORDER BY
    TRY_CONVERT(INT, seq_pick.SeqValue) DESC,
    TRY_CONVERT(INT, SUBSTRING(p.catalogueId, 4, LEN(p.catalogueId) - 3)) DESC
";
        //Query alternativa: SIN WorkArea (pero con el MISMO schema que espera SqlToJsonConverter)
        private const string consultaSG1_BOP_SinWorkArea = @"
WITH FirstProcessName AS (
    SELECT TOP (1)
        CASE 
            WHEN LEN(p.catalogueId) != 8 THEN RIGHT(p.catalogueId, LEN(p.catalogueId) - 2)
            ELSE RIGHT(p.catalogueId, 6) 
        END AS first_process_name
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
                THEN ISNULL(qty_pick.QtyValue, 0)
            ELSE 1
        END AS Qty
    FROM Process p
    LEFT JOIN ProcessRevision pr
        ON pr.masterRef = p.id_Table
    LEFT JOIN ProcessOccurrence po
        ON po.instancedRef = pr.id_Table
    LEFT JOIN ProcessOccurrence po_op
        ON po_op.parentRef = po.id_Table
       AND po_op.idXml = po.idXml
    LEFT JOIN Occurrence o
        ON (
            o.parentRef = po.id_Table
            OR o.parentRef = po_op.id_Table
        )
       AND o.idXml = po.idXml
       AND ISNULL(o.subType,'') <> 'METool'
    LEFT JOIN ProductRevision prod_rev
        ON prod_rev.id_Table = o.instancedRef
       AND prod_rev.idXml    = o.idXml
    LEFT JOIN Product prod
        ON prod.id_Table = prod_rev.masterRef
       AND prod.idXml    = prod_rev.idXml
    OUTER APPLY (
        SELECT TOP (1) q.QtyValue
        FROM (
            SELECT
                TRY_CONVERT(FLOAT, REPLACE(NULLIF(LTRIM(RTRIM(uvO.value)), ''), ',', '.')) AS QtyValue,
                1 AS prio
            FROM UserValue_Occurrence uvO
            WHERE uvO.id_Father = o.id_Table
              AND uvO.title IN ('Quantity','quantity')
              AND (uvO.idXml = o.idXml OR uvO.idXml IS NULL)

            UNION ALL

            SELECT
                TRY_CONVERT(FLOAT, REPLACE(NULLIF(LTRIM(RTRIM(uvUD.value)), ''), ',', '.')) AS QtyValue,
                2 AS prio
            FROM UserData ud
            JOIN UserValue_UserData uvUD
              ON uvUD.id_Father = ud.id_Table
             AND uvUD.title IN ('Quantity','quantity')
             AND (uvUD.idXml = o.idXml OR uvUD.idXml IS NULL)
            WHERE ud.id_Father = o.id_Table
              AND (ud.idXml = o.idXml OR ud.idXml IS NULL)

            UNION ALL

            SELECT
                TRY_CONVERT(FLOAT, REPLACE(NULLIF(LTRIM(RTRIM(uvUD2.value)), ''), ',', '.')) AS QtyValue,
                3 AS prio
            FROM UserValue_UserData uvUD2
            WHERE uvUD2.id_Father = o.id_Table + 2
              AND uvUD2.title IN ('Quantity','quantity')
              AND (uvUD2.idXml = o.idXml OR uvUD2.idXml IS NULL)
        ) q
        WHERE q.QtyValue IS NOT NULL
        ORDER BY q.prio
    ) qty_pick
    WHERE
        prod.productId IS NOT NULL
        AND prod_rev.subType NOT IN ('Mfg0MEResourceRevision', 'Mfg0MEFactoryToolRevision')
        AND NOT (
            prod_rev.subType IN ('Agm4_MatPrimaRevision','Agm4_RepCompradoRevision')
            AND ISNULL(qty_pick.QtyValue, 0) <= 0
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
    seq_pick.SeqValue AS num_busqueda,
    NULL AS WorkArea_CatalogueId,
    NULL AS WorkArea_Nombre,
    NULL AS WorkArea_Revision
FROM Process p
LEFT JOIN ProcessRevision pr
    ON pr.masterRef = p.id_Table
LEFT JOIN ProcessOccurrence po
    ON po.instancedRef = pr.id_Table

OUTER APPLY (
    SELECT TOP (1) s.SeqValue
    FROM (
        SELECT
            NULLIF(LTRIM(RTRIM(uvpo.value)), '') AS SeqValue,
            1 AS prio
        FROM UserValue_ProcessOccurrence uvpo
        WHERE uvpo.id_Father = po.id_Table
          AND uvpo.title IN ('SequenceNumber','sequencenumber','sequenceNumber')
          AND (uvpo.idXml = po.idXml OR uvpo.idXml IS NULL)

        UNION ALL

        SELECT
            NULLIF(LTRIM(RTRIM(uvpo2.value)), '') AS SeqValue,
            2 AS prio
        FROM UserValue_ProcessOccurrence uvpo2
        WHERE uvpo2.id_Father = po.id_Table
          AND uvpo2.title LIKE '%Sequence%'
          AND (uvpo2.idXml = po.idXml OR uvpo2.idXml IS NULL)

        UNION ALL

        SELECT
            NULLIF(LTRIM(RTRIM(uv.value)), '') AS SeqValue,
            3 AS prio
        FROM UserData ud
        JOIN UserValue_UserData uv
          ON uv.id_Father = ud.id_Table
         AND uv.title IN ('SequenceNumber','sequencenumber')
         AND (uv.idXml = po.idXml OR uv.idXml IS NULL)
        WHERE ud.id_Father = po.id_Table
          AND (ud.idXml = po.idXml OR ud.idXml IS NULL)

        UNION ALL

        SELECT
            NULLIF(LTRIM(RTRIM(uud2.value)), '') AS SeqValue,
            4 AS prio
        FROM UserValue_UserData uud2
        WHERE uud2.id_Father = po.id_Table + 2
          AND uud2.title IN ('SequenceNumber','sequencenumber')
          AND (uud2.idXml = po.idXml OR uud2.idXml IS NULL)

        UNION ALL

        SELECT
            NULLIF(LTRIM(RTRIM(uud3.value)), '') AS SeqValue,
            5 AS prio
        FROM UserValue_UserData uud3
        WHERE uud3.id_Father = po.id_Table
          AND uud3.title IN ('SequenceNumber','sequencenumber')
          AND (uud3.idXml = po.idXml OR uud3.idXml IS NULL)
    ) s
    WHERE s.SeqValue IS NOT NULL
    ORDER BY s.prio
) seq_pick

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
    seq_pick.SeqValue
ORDER BY
    TRY_CONVERT(INT, seq_pick.SeqValue) DESC,
	TRY_CONVERT(INT, SUBSTRING(p.catalogueId, 4, LEN(p.catalogueId) - 3)) DESC;

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

        public static async Task postSG1(
    Dictionary<string, List<List<Dictionary<string, string>>>> estructurasBopCompleta,
    bool yaHizoResetGlobal = false)
        {
            var putFallBack = new Dictionary<string, List<List<Dictionary<string, string>>>>();

            Console.WriteLine($"[SG1-POST] Iniciando envío masivo. Productos: {estructurasBopCompleta.Count}");
            Utilidades.EscribirEnLog($"[SG1-POST] Iniciando envío masivo. Productos: {estructurasBopCompleta.Count}");

            foreach (var parent in estructurasBopCompleta)
            {
                string producto = parent.Key;
                int cantItems = parent.Value?.Count ?? 0;

                var jsonBody = new
                {
                    producto = producto,
                    qtdBase = "1",
                    estructura = parent.Value
                };

                string jsonData = JsonConvert.SerializeObject(jsonBody, Formatting.Indented);

                Utilidades.EscribirJSONEnLog($"[SG1-POST] JSON COMPLETO para producto {producto}:\n{jsonData}");

                // Usá tu helper con retry infinito para 5xx/transitorios
                var (statusCode, httpStatus, responseData) = await PostSG1RawAsync(producto, jsonData);

                // 409 => acumular para PUT
                if (httpStatus == HttpStatusCode.Conflict)
                {
                    putFallBack[producto] = parent.Value;
                    ActualizarBase(statusCode, responseData, producto);
                    continue;
                }

                // OK
                if ((int)httpStatus >= 200 && (int)httpStatus <= 299)
                {
                    ActualizarBase(statusCode, responseData, producto);
                    continue;
                }

                // Otros 4xx (o lo que sea) => registrar y seguir
                ActualizarBase(statusCode, responseData, producto);
            }

            if (putFallBack.Count > 0)
            {
                Utilidades.EscribirEnLog($"[SG1-POST] POST finalizado. 409 acumulados={putFallBack.Count}. Disparando PUT...");
                await putSG1(putFallBack, estructurasBopCompleta, yaHizoResetGlobal);
            }
            else
            {
                Utilidades.EscribirEnLog("[SG1-POST] POST finalizado. Sin 409.");
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
            //string connectionString = "Server=10.0.0.82;Database=AgrometalBOP;User Id=sa;Password=Descar_2020;";
            string connectionString = "Server=SRV-TEAMCENTER;Database=MBOM-BOP_Agrometal;User Id=infodba;Password=infodba;";
            // ✅ Elegir query según compatibilidad del XML/BD
            string queryElegida;
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                queryElegida = ObtenerConsultaSG1_BOP(conn);
                //Utilidades.EscribirEnLog("SG1 -> Query elegida:\n" + queryElegida);
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


        public static async Task putSG1(
    Dictionary<string, List<List<Dictionary<string, string>>>> estructuras409,
    Dictionary<string, List<List<Dictionary<string, string>>>> estructurasBopCompleta,
    bool yaHizoResetGlobal)
        {
            Console.WriteLine($"[SG1-PUT] Iniciando PUT masivo. Cantidad de productos: {estructuras409.Count}");
            Utilidades.EscribirEnLog($"[SG1-PUT] Iniciando PUT masivo. Cantidad de productos: {estructuras409.Count}");

            bool recursividadDetectada = false;
            string? codigoQueDisparo = null;
            string? bodyQueDisparo = null;

            foreach (var parent in estructuras409)
            {
                string producto = parent.Key;

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
                    continue;

                int intento = 0;

                while (true)
                {
                    intento++;

                    int statusCode = 0;
                    string responseData = string.Empty;

                    try
                    {
                        await ProtheusHealth.WaitUntilActiveAsync("SG1-PUT", RetryDelay);
                        using var content = new StringContent(jsonData, Encoding.UTF8, "application/json");
                        using HttpResponseMessage response = await _clientSG1.PutAsync(SG1_PUT_URL, content);

                        statusCode = (int)response.StatusCode;
                        responseData = await response.Content.ReadAsStringAsync();
                        LogErrorProtheusIfAny("SG1", "PUT", codigo, responseData);

                        if (!response.IsSuccessStatusCode &&
                            !IsRetryableStatus(response.StatusCode) &&
                            IsRecursividadError(responseData))
                        {
                            recursividadDetectada = true;
                            codigoQueDisparo = codigo;
                            bodyQueDisparo = responseData;

                            Utilidades.EscribirEnLog($"[SG1-PUT] Recursividad detectada en {codigo}. Se solicita RESET global de la BOP.");
                            break; // salgo del while
                        }

                        if (IsRetryableStatus(response.StatusCode))
                        {
                            Utilidades.EscribirEnLog($"[SG1-PUT] {statusCode} para {codigo}. Reintento en 5 minutos. Intento #{intento}. Resp: {responseData}");
                            await Task.Delay(RetryDelay);
                            continue;
                        }

                        if (response.IsSuccessStatusCode)
                        {
                            ActualizarBase(statusCode, responseData, codigo);
                            break;
                        }

                        ActualizarBase(statusCode, responseData, codigo);
                        break;
                    }
                    catch (Exception ex) when (IsRetryableException(ex))
                    {
                        Utilidades.EscribirEnLog($"[SG1-PUT] EXCEPCIÓN transitoria para {codigo}: {ex.Message}. Reintento en 5 minutos. Intento #{intento}");
                        await Task.Delay(RetryDelay);
                        continue;
                    }
                    catch (Exception ex)
                    {
                        ActualizarBase(0, ex.ToString(), codigo);
                        break;
                    }
                }

                if (recursividadDetectada)
                    break; // salgo del foreach
            }

            if (!recursividadDetectada)
            {
                Utilidades.EscribirEnLog("[SG1-PUT] PUT masivo finalizado sin recursividad.");
                return;
            }

            // Si ya se hizo un reset global, no vuelvo a repetir para evitar loop infinito
            if (yaHizoResetGlobal)
            {
                Utilidades.EscribirEnLog($"[SG1-RESET] Recursividad volvió a ocurrir (disparó {codigoQueDisparo}). Ya se hizo reset global antes, se aborta reproceso. Body: {bodyQueDisparo}");
                if (!string.IsNullOrWhiteSpace(codigoQueDisparo))
                    ActualizarBase(0, $"Recursividad persistente. Body={bodyQueDisparo}", codigoQueDisparo);
                return;
            }

            Utilidades.EscribirEnLog($"[SG1-RESET] Iniciando RESET global por recursividad. Disparó={codigoQueDisparo}. Se eliminarán {estructurasBopCompleta.Count} estructuras.");

            var okDelete = await EliminarEstructurasBopAsync(estructurasBopCompleta.Keys);

            Utilidades.EscribirEnLog($"[SG1-RESET] RESET global finalizado. okDelete={okDelete}. Se reprocesa BOP completa.");

            // Reprocesar BOP completa (POST + PUT fallback) una sola vez
            await postSG1(estructurasBopCompleta, yaHizoResetGlobal: true);
        }



        static void AgregarFechaAServiciosTerceros(
    Dictionary<string, List<List<Dictionary<string, string>>>> estructuras,
    DateTime fechaEnvio)
        {
            if (estructuras == null || estructuras.Count == 0) return;

            // Fecha fija: 01/01/2025
            const string fechaStr = "20250101"; // formato yyyyMMdd para Protheus

            foreach (var kv in estructuras)
            {
                var estructura = kv.Value;
                if (estructura == null) continue;

                foreach (var bloque in estructura)
                {
                    if (bloque == null || bloque.Count == 0) continue;

                    string? codigo = bloque
                        .FirstOrDefault(d =>
                            d != null &&
                            d.TryGetValue("campo", out var c) &&
                            string.Equals(c, "codigo", StringComparison.OrdinalIgnoreCase))
                        ?.GetValueOrDefault("valor");

                    if (!EsPRT(codigo)) continue;

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

            //string connectionString = "Server=10.0.0.82;Database=AgrometalBOP;User Id=sa;Password=Descar_2020;";
            string connectionString = "Server=SRV-TEAMCENTER;Database=MBOM-BOP_Agrometal;User Id=infodba;Password=infodba;";
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
                        command.CommandTimeout = 300;
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
                                        //Console.WriteLine($"[SG1-JSON] Fila raíz detectada → ParentCodigo={parentCodigo}, ChildCodigo={childCodigo}");
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

                                //Console.WriteLine("[JSON MBOM]");
                                //Console.WriteLine(json);
                                Utilidades.EscribirJSONEnLog("[SG1 - JSON MBOM]");
                                Utilidades.EscribirJSONEnLog(json);
                                // (opcional) a archivo para no saturar consola
                                //File.WriteAllText("mbom_debug.json", json);
                                //Console.WriteLine("[MBOM] Dump a mbom_debug.json");

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

            //string connectionString = "Server=10.0.0.82;Database=AgrometalBOP;User Id=sa;Password=Descar_2020;";
            string connectionString = "Server=SRV-TEAMCENTER;Database=MBOM-BOP_Agrometal;User Id=infodba;Password=infodba;";
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

            //string connectionString = "Server=10.0.0.82;Database=AgrometalBOP;User Id=sa;Password=Descar_2020;";
            string connectionString = "Server=SRV-TEAMCENTER;Database=MBOM-BOP_Agrometal;User Id=infodba;Password=infodba;";
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
