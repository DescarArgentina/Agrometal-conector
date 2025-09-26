using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net.Http.Headers;
using System.Reflection;
using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Web_Service
{
    public class Tablas_SG2_SH3
    {

        public sealed class WsError
        {
            public int? errorCode { get; set; }
            public string errorMessage { get; set; }
        }
        public static async Task postSG2_SH3(string jsonData)
        {
            string url = "http://119.8.73.193:8086/rest/TCProceso/Incluir"; // URL del servicio REST
            
            string username = "USERREST"; // Usuario proporcionado
            string password = "restagr"; // Contraseña proporcionada

            //string username = "USERREST"; // Usuario proporcionado
            //string password = "restagr"; // Contraseña proporcionada

            using (HttpClient client = new HttpClient())
            {
                try
                {
                    // Configurar credenciales Basic Auth
                    var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
                    client.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

                    // Configurar el contenido de la solicitud
                    var content = new StringContent(jsonData, Encoding.UTF8, "application/json");

                    // Realizar la solicitud POST
                    HttpResponseMessage response = await client.PostAsync(url, content);

                    // Leer la respuesta como string
                    string responseData = await response.Content.ReadAsStringAsync();

                    Console.WriteLine($"POST TCProceso -> {(int)response.StatusCode} {response.ReasonPhrase}\n{responseData}");

                    if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                    {
                        Console.WriteLine("Ya existe (409). Intentando PUT /Modificar...");
                        await putSG2_SH3(jsonData);     // llamada a la función PUT
                        return;                         // salimos después del PUT
                        //WsError err = null;
                        //try { err = JsonConvert.DeserializeObject<WsError>(responseData); } catch { }

                        //switch (err?.errorCode)
                        //{
                        //    case 3:
                        //        Console.WriteLine("Ya existe: hago PUT /Modificar/...");
                        //        await putSG2_SH3(jsonData);
                        //        return;
                        //    case 2:
                        //        Console.WriteLine("El producto no existe en el ERP. Primero crear/activar SB1 con TCProductos/Incluir/.");
                        //        return;
                        //    case 8:
                        //        Console.WriteLine("No existe para modificar: revisar 'código' y 'producto' enviados antes de intentar el PUT");
                        //        return;
                        //    default:
                        //        Console.WriteLine("409 no reconocido por código: revisar el payload y matriz de campos");
                        //        return;
                        //}
                    }

                    // Si no fue 409, ahora sí validamos éxito del POST
                    if (!response.IsSuccessStatusCode)
                    {
                        //    var bodyErr = await response.Content.ReadAsStringAsync();
                        Console.WriteLine($"POST falló (no 409). Ver detalle arriba.");
                        //    return;
                    }
                    else
                    {
                        Console.WriteLine("POST ok.");
                    }

                    // Asegurarse de que la respuesta sea exitosa
                    //response.EnsureSuccessStatusCode();

                    // Leer la respuesta como string
                    //string responseData = await response.Content.ReadAsStringAsync();

                    // Mostrar la respuesta en consola
                    Console.WriteLine("Respuesta del servicio:");
                    Console.WriteLine(responseData);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error al consumir el servicio: {ex.Message}");
                }
            }
        }



        public static async Task putSG2_SH3(string jsonData)
        {
            string url = "http://119.8.73.193:8086/rest/TCProceso/Modificar/";
            string username = "USERREST";
            string password = "restagr";

            using (var client = new HttpClient())
            {
                try
                {
                    var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
                    client.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

                    var content = new StringContent(jsonData, Encoding.UTF8, "application/json");

                    HttpResponseMessage response = await client.PutAsync(url, content);

                    if (!response.IsSuccessStatusCode)
                    {
                        var bodyErr = await response.Content.ReadAsStringAsync();
                        Console.WriteLine($"PUT falló: {(int)response.StatusCode} {response.ReasonPhrase}\n{bodyErr}");
                        return;
                    }

                    string responseData = await response.Content.ReadAsStringAsync();
                    Console.WriteLine("Respuesta del servicio (PUT ok):");
                    Console.WriteLine(responseData);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error al consumir el servicio: {ex.Message}");
                }
            }
        }

        static string NormalizarCodigo(string codigo)
        {
            if (string.IsNullOrWhiteSpace(codigo)) return codigo;
            codigo = codigo.Trim();

            // Casos especiales (no es “poner 0”, pero suelen ir juntos)
            if (codigo == "000485") return "481708";
            if (codigo == "000402") return "482918";

            // Códigos que llevan un 0 insertado después del 3er dígito
            if (codigo == "45356" || codigo == "45357" || codigo == "45545" || codigo == "45459" || codigo == "49877" || codigo == "16486" || codigo == "52142" ||
                codigo == "45730" || codigo == "45796" || codigo == "45547" || codigo == "45553" || codigo == "34884" || codigo == "48079" || codigo == "52414")
            {
                return codigo.Insert(3, "0");
            }

            // Opcional: si es numérico y tiene <6 dígitos, completar a 6
            //if (codigo.All(char.IsDigit) && codigo.Length < 6)
            //    return codigo.PadLeft(6, '0');

            return codigo;
        }

        public static List<string> jsonSG2_SH3()
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                      Integrated Security=True;TrustServerCertificate=True";


            //string query = @"SELECT
            //    p.catalogueId  AS Padre,          -- Proceso
            //    op.catalogueId AS Hijo,           -- Operación
            //    src.recurso    AS recurso,        -- productId o catalogueId (según origen)
            //    uud.value,
            //    calc.tiempo_fmt AS tiempo,        -- << reglas exactas solicitadas
            //    op.name         AS descripcion,
            //    calc.lote_val   AS loteStd        -- << reglas exactas solicitadas
            //FROM Operation               AS op
            //JOIN OperationRevision       AS opr    ON opr.masterRef      = op.id_Table
            //JOIN ProcessOccurrence       AS po_op  ON po_op.instancedRef = opr.id_Table          -- PO de la operación
            //JOIN ProcessOccurrence       AS po_pr  ON po_pr.id_Table     = po_op.parentRef       -- PO del proceso
            //JOIN ProcessRevision         AS prev   ON prev.id_Table      = po_pr.instancedRef
            //JOIN Process                 AS p      ON p.id_Table         = prev.masterRef

            ///* ----- Tiempo (según Forms) ----- */
            //JOIN Form AS f_op
            //  ON f_op.name = CONCAT(op.catalogueId, '/', opr.revision)
            //JOIN Form AS f_time
            //  ON f_time.id_Table = f_op.id_Table + 3
            //JOIN UserValue_UserData AS uud
            //  ON (
            //       uud.id_Father = f_time.id_Table + 1
            //     )
            // AND uud.title = 'allocated_time'

            ///* ======= NORMALIZACIÓN DE TIEMPO Y LOTE SEGÚN TUS CONDICIONES ======= */
            //CROSS APPLY (SELECT t = COALESCE(TRY_CAST(uud.value AS float), 0.0)) AS x
            //CROSS APPLY (
            //    SELECT
            //        tiempo_fmt = CASE
            //            WHEN x.t = 0.0
            //                THEN '00.01'
            //            WHEN x.t < 60
            //                THEN REPLACE(CONVERT(varchar(5), DATEADD(SECOND, x.t * 60, 0), 108), ':', '.')
            //            ELSE REPLACE(CONVERT(varchar(5), DATEADD(SECOND, x.t, 0), 108), ':', '.')
            //        END,
            //        lote_val = CASE
            //            WHEN x.t < 60 THEN 60 ELSE 1
            //        END
            //) AS calc

            ///* ----- Recursos colgados de la operación/proceso (Occurrence.parentRef en ambos niveles) ----- */
            //OUTER APPLY (
            //    SELECT TOP 1
            //        r.recurso,
            //        r.tipo_recurso
            //    FROM (
            //        /* Product / Herramental / Máquina (prioridad 1) - hijos del PROCESO */
            //        SELECT
            //            occ.parentRef,
            //            CAST(prod.productId AS varchar(50)) AS recurso,
            //            prod.subType AS tipo_recurso,
            //            1 AS prio
            //        FROM Occurrence AS occ
            //        JOIN ProductRevision AS prodrev
            //          ON prodrev.id_Table = occ.instancedRef AND prodrev.idXml = occ.idXml
            //        JOIN Product AS prod
            //          ON prod.id_Table    = prodrev.masterRef AND prod.idXml    = prodrev.idXml
            //        WHERE occ.idXml   = po_pr.idXml
            //          AND occ.parentRef = po_pr.id_Table      -- << SOLO bajo el proceso

            //        UNION ALL

            //        /* WorkArea (prioridad 2, excluye TERCEROS) - hijos del PROCESO */
            //        SELECT
            //            wao.parentRef,
            //            CAST(wa.catalogueId AS varchar(50)) AS recurso,
            //            'MEWorkArea' AS tipo_recurso,
            //            2 AS prio
            //        FROM WorkArea AS wa
            //        JOIN WorkAreaRevision AS war
            //          ON war.masterRef = wa.id_Table  AND war.idXml  = wa.idXml
            //        JOIN Occurrence AS wao
            //          ON wao.instancedRef = war.id_Table AND wao.idXml = war.idXml
            //        WHERE wao.idXml   = po_pr.idXml
            //          AND wao.parentRef = po_pr.id_Table   -- << SOLO bajo el proceso
            //          AND COALESCE(war.name, wa.name) COLLATE Latin1_General_CI_AI NOT LIKE '%TERCEROS%'
            //    ) AS r
            //    /* Priorizar MEResource / METool / MENCMachine sobre WorkArea */
            //    ORDER BY CASE
            //                WHEN r.tipo_recurso IN ('MEResource','Mfg0MEResource','METool','Mfg0METool','MENCMachine','Mfg0MENCMachine') THEN 0
            //                ELSE 1
            //             END,
            //             r.prio
            //) AS src

            ///* ----- Excluir operaciones “TERCEROS …” ----- */
            //WHERE p.catalogueId LIKE 'PR%'
            //  AND op.name COLLATE Latin1_General_CI_AI NOT LIKE '%TERCEROS%'
            //  AND src.recurso IS NOT NULL";


            string query = @"
            SELECT
    p.catalogueId  AS Padre,          -- Proceso
    op.catalogueId AS Hijo,           -- Operación
    src.recurso    AS recurso,        -- productId o catalogueId (según origen)
    uud.value,
    calc.tiempo_fmt AS tiempo,        -- reglas de formato
    op.name         AS descripcion,
    calc.lote_val   AS loteStd
FROM Operation               AS op
JOIN OperationRevision       AS opr    ON opr.masterRef      = op.id_Table
JOIN ProcessOccurrence       AS po_op  ON po_op.instancedRef = opr.id_Table
JOIN ProcessOccurrence       AS po_pr  ON po_pr.id_Table     = po_op.parentRef
JOIN ProcessRevision         AS prev   ON prev.id_Table      = po_pr.instancedRef
JOIN Process                 AS p      ON p.id_Table         = prev.masterRef

/* ----- FIX: resolver el nombre del Form de la operación sin '-OP' ----- */
CROSS APPLY (
  SELECT CASE
           WHEN RIGHT(op.catalogueId, 3) = '-OP' THEN LEFT(op.catalogueId, LEN(op.catalogueId) - 3)
           WHEN CHARINDEX('-OP', op.catalogueId) > 0 THEN REPLACE(op.catalogueId, '-OP', '')
           ELSE op.catalogueId
         END AS op_code_base
) AS x
JOIN Form AS f_op
  ON f_op.name = CONCAT(x.op_code_base, '/', opr.revision)

/* ----- Tiempo (según Forms) [mantenemos tu lógica actual de offsets] ----- */
JOIN Form AS f_time
  ON f_time.id_Table = f_op.id_Table + 3
JOIN UserValue_UserData AS uud
  ON uud.id_Father = f_time.id_Table + 1
 AND uud.title = 'allocated_time'

/* ======= Normalización de tiempo y lote ======= */
CROSS APPLY (SELECT t = COALESCE(TRY_CAST(uud.value AS float), 0.0)) AS x2
CROSS APPLY (
    SELECT
        tiempo_fmt = CASE
            WHEN x2.t = 0.0 THEN '0.01'
            WHEN x2.t < 60 THEN REPLACE(CONVERT(varchar(5), DATEADD(SECOND, x2.t * 60, 0), 108), ':', '.')
            ELSE REPLACE(CONVERT(varchar(5), DATEADD(SECOND, x2.t, 0), 108), ':', '.')
        END,
        lote_val = CASE WHEN x2.t < 60 THEN 60 ELSE 1 END
) AS calc

/* ----- Recursos colgados del proceso ----- */
OUTER APPLY (
    SELECT TOP (1)
        r.recurso,
        r.tipo_recurso,
        r.src_from   -- para depurar: 'PROC' o 'OP'
    FROM (
        /* 1) Bajo el ProcessOccurrence (hermano de la OP) */
        SELECT
            occ.parentRef,
            CAST(prod.productId AS varchar(50)) AS recurso,
            prod.subType                         AS tipo_recurso,
            'PROC'                               AS src_from,
            1                                    AS prio
        FROM dbo.Occurrence        AS occ
        JOIN dbo.ProductRevision   AS prodRev ON prodRev.id_Table = occ.instancedRef AND prodRev.idXml = occ.idXml
        JOIN dbo.Product           AS prod    ON prod.id_Table    = prodRev.masterRef AND prod.idXml   = prodRev.idXml
        WHERE occ.parentRef = po_pr.id_Table
          AND occ.idXml    = po_pr.idXml
          AND prod.subType IN ('MEResource','Mfg0MEResource','METool','Mfg0METool','MENCMachine','Mfg0MENCMachine')

        UNION ALL
        SELECT
            wao.parentRef,
            CAST(wa.catalogueId AS varchar(50)) AS recurso,
            'MEWorkArea'                        AS tipo_recurso,
            'PROC'                              AS src_from,
            2                                   AS prio
        FROM dbo.WorkArea           AS wa
        JOIN dbo.WorkAreaRevision   AS waRev ON waRev.masterRef = wa.id_Table  AND waRev.idXml = wa.idXml
        JOIN dbo.Occurrence         AS wao   ON wao.instancedRef = waRev.id_Table AND wao.idXml = waRev.idXml
        WHERE wao.parentRef = po_pr.id_Table
          AND wao.idXml    = po_pr.idXml
          AND COALESCE(waRev.name, wa.name) COLLATE Latin1_General_CI_AI NOT LIKE '%TERCEROS%'

        /* 2) Fallback: bajo el OperationOccurrence (hijo de la OP) */
        UNION ALL
        SELECT
            occ.parentRef,
            CAST(prod.productId AS varchar(50)) AS recurso,
            prod.subType                         AS tipo_recurso,
            'OP'                                 AS src_from,
            3                                    AS prio
        FROM dbo.Occurrence        AS occ
        JOIN dbo.ProductRevision   AS prodRev ON prodRev.id_Table = occ.instancedRef AND prodRev.idXml = occ.idXml
        JOIN dbo.Product           AS prod    ON prod.id_Table    = prodRev.masterRef AND prod.idXml   = prodRev.idXml
        WHERE occ.parentRef = po_op.id_Table
          AND occ.idXml    = po_op.idXml
          AND prod.subType IN ('MEResource','Mfg0MEResource','METool','Mfg0METool','MENCMachine','Mfg0MENCMachine')
        -- ojo: NO incluimos materias primas (ej. 'Agm4_MatPrima', 'MEConsumed')

 UNION ALL
        SELECT
            wao.parentRef,
            CAST(wa.catalogueId AS varchar(50)) AS recurso,
            'MEWorkArea'                        AS tipo_recurso,
            'OP'                                AS src_from,
            4                                   AS prio
        FROM dbo.WorkArea           AS wa
        JOIN dbo.WorkAreaRevision   AS waRev ON waRev.masterRef = wa.id_Table  AND waRev.idXml = wa.idXml
        JOIN dbo.Occurrence         AS wao   ON wao.instancedRef = waRev.id_Table AND wao.idXml = waRev.idXml
        WHERE wao.parentRef = po_op.id_Table
          AND wao.idXml    = po_op.idXml
          AND COALESCE(waRev.name, wa.name) COLLATE Latin1_General_CI_AI NOT LIKE '%TERCEROS%'
    ) AS r
    ORDER BY 
        CASE WHEN r.tipo_recurso IN ('MEResource','Mfg0MEResource','METool','Mfg0METool','MENCMachine','Mfg0MENCMachine') THEN 0 ELSE 1 END,
        r.prio
) AS src

/* ----- Filtros ----- */
WHERE p.catalogueId LIKE 'PR%'
  AND op.name COLLATE Latin1_General_CI_AI NOT LIKE '%TERCEROS%'
  AND src.recurso IS NOT NULL";





            List<string> jsonProductos = new List<string>();

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    SqlCommand command = new SqlCommand(query, connection);
                    connection.Open();
                    SqlDataReader reader = command.ExecuteReader();

                    Dictionary<string, dynamic> productosDict = new Dictionary<string, dynamic>();
                    Dictionary<string, Dictionary<string, int>> productoRecursoOperacion = new Dictionary<string, Dictionary<string, int>>();

                    while (reader.Read())
                    {
                        string producto = reader["Padre"]?.ToString();
                        string codigo = "01";

                        string tiempo = reader["tiempo"]?.ToString().Replace(',', '.');
                        if (decimal.TryParse(tiempo, NumberStyles.Any, CultureInfo.InvariantCulture, out var valorDecimal))
                        {
                            tiempo = valorDecimal.ToString("0.##", CultureInfo.InvariantCulture);
                        }

                        // Blind-guard por si viniera vacío o 0
                        if (string.IsNullOrWhiteSpace(tiempo) || tiempo == "0" || tiempo == "0.00" || tiempo == "00.00")
                            tiempo = "00.01";   // cumple la regla de Protheus


                        string recurso = reader["recurso"]?.ToString();
                        recurso = NormalizarCodigo(recurso);
                        string nombreOperacion = reader["descripcion"]?.ToString();
                        string lote = reader["loteStd"]?.ToString();

                        // Asignar operacion (incrementa por recurso)
                        if (!productoRecursoOperacion.ContainsKey(producto))
                        {
                            productoRecursoOperacion[producto] = new Dictionary<string, int>();
                        }

                        int operacionActual = 10;
                        if (productoRecursoOperacion[producto].ContainsKey(recurso))
                        {
                            operacionActual = productoRecursoOperacion[producto][recurso] + 10;
                        }
                        productoRecursoOperacion[producto][recurso] = operacionActual;

                        string operacion = operacionActual.ToString("D2"); // Formato 0010, 0020, etc.

                        var procedimiento = new Procedimiento
                        {
                            detalle = new List<CampoValor>
                    {
                        new CampoValor { campo = "operacion", valor = operacion },
                        new CampoValor { campo = "recurso", valor = recurso },
                        new CampoValor { campo = "tiempo", valor = tiempo },
                        new CampoValor { campo = "descripcion", valor = nombreOperacion },
                        new CampoValor { campo = "loteStd", valor = lote }
                    }
                        };

                        if (productosDict.ContainsKey(producto))
                        {
                            productosDict[producto].procedimiento.Add(procedimiento);
                        }
                        else
                        {
                            productosDict[producto] = new
                            {
                                codigo = codigo,
                                producto = producto,
                                procedimiento = new List<Procedimiento> { procedimiento }
                            };
                        }
                    }

                    foreach (var item in productosDict.Values)
                    {
                        string json = JsonConvert.SerializeObject(item, Formatting.Indented);
                        Console.WriteLine(json);
                        jsonProductos.Add(json);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }

            return jsonProductos;

        }



        //    List<string> jsonProductos = new List<string>();
        //    try
        //    {
        //        using (SqlConnection connection = new SqlConnection(connectionString))
        //        {
        //            SqlCommand command = new SqlCommand(query, connection);
        //            connection.Open();
        //            SqlDataReader reader = command.ExecuteReader();

        //            Dictionary<string, dynamic> productosDict = new Dictionary<string, dynamic>();

        //            while (reader.Read())
        //            {
        //                string producto = reader["codigo"].ToString();
        //                string codigo = "01"; // Se mantiene como constante por ahora

        //                string tiempo = reader["tiempo"].ToString().Replace(',','.');
        //                if (decimal.TryParse(tiempo, NumberStyles.Any, CultureInfo.InvariantCulture, out var valorDecimal))
        //                {
        //                    // 3. Redondeamos/truncamos a 2 decimales
        //                    //    - Para redondear: Math.Round(valorDecimal, 2, MidpointRounding.AwayFromZero)
        //                    //    - Aquí usamos ToString("0.##") para mostrar hasta 2 decimales (no rellena ceros)
        //                    tiempo = valorDecimal
        //                        .ToString("0.##", CultureInfo.InvariantCulture);
        //                }
        //                string operacion = reader["Operacion"].ToString();
        //                string recurso = reader["instancedWorkArea"].ToString();
        //                string nombreOperacion = reader["Operation_name"].ToString();
        //                string lote = reader["loteStd"].ToString();

        //                // Crear el procedimiento
        //                var procedimiento = new Procedimiento
        //                {
        //                    detalle = new List<CampoValor>
        //        {
        //            new CampoValor { campo = "operacion", valor = operacion },
        //            new CampoValor { campo = "recurso", valor = recurso },
        //            new CampoValor { campo = "tiempo", valor = tiempo },
        //            new CampoValor {campo = "descripcion", valor = nombreOperacion},
        //            new CampoValor {campo= "loteStd", valor = lote }
        //        }
        //                };

        //                // Verificar si el producto ya está en el diccionario
        //                //if (productosDict.ContainsKey(producto))
        //                //{
        //                //    productosDict[producto].procedimiento.Add(procedimiento);
        //                //}
        //                //else
        //                //{
        //                //    productosDict[producto] = new
        //                //    {
        //                //        codigo = codigo,
        //                //        producto = producto,
        //                //        procedimiento = new List<Procedimiento> { procedimiento }
        //                //    };
        //                //}


        //                productosDict[producto] = new
        //                {
        //                    codigo = codigo,
        //                    producto = producto,
        //                    procedimiento = new List<Procedimiento> { procedimiento }
        //                };


        //            }

        //            // Convertir cada elemento del diccionario en JSON
        //            foreach (var item in productosDict.Values)
        //            {
        //                string json = JsonConvert.SerializeObject(item, Formatting.Indented);
        //                Console.WriteLine(json);
        //                jsonProductos.Add(json);
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine("Error: " + ex.Message);
        //    }

        //    return jsonProductos;
        //}

        public static List<string> jsonSB1_BOP()
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                        Integrated Security=True;TrustServerCertificate=True";

            // 1) Batch para endurecer el esquema (idempotente)
            const string schemaPatch = @"
                                        SET NOCOUNT ON;
                                        SET XACT_ABORT ON;
                                        BEGIN TRAN;

                                        DECLARE @schema sysname, @sql nvarchar(max);

                                        -- ===== Occurrence =====
                                        SELECT TOP(1) @schema = s.name
                                        FROM sys.tables t
                                        JOIN sys.schemas s ON s.schema_id = t.schema_id
                                        WHERE t.name = N'Occurrence';

                                        IF @schema IS NOT NULL
                                        BEGIN
                                            IF NOT EXISTS (
                                                SELECT 1
                                                FROM sys.columns c
                                                WHERE c.object_id = OBJECT_ID(QUOTENAME(@schema)+N'.'+QUOTENAME(N'Occurrence'))
                                                  AND LOWER(c.name) = N'subtype'
                                            )
                                            BEGIN
                                                SET @sql = N'ALTER TABLE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(N'Occurrence')
                                                         + N' ADD ' + QUOTENAME(N'subType') + N' NVARCHAR(100) NULL;';
                                                EXEC sp_executesql @sql;
                                            END
                                        END

                                        -- ===== ProcessRevision =====
                                        SET @schema = NULL;
                                        SELECT TOP(1) @schema = s.name
                                        FROM sys.tables t
                                        JOIN sys.schemas s ON s.schema_id = t.schema_id
                                        WHERE t.name = N'ProcessRevision';

                                        IF @schema IS NOT NULL
                                        BEGIN
                                            IF NOT EXISTS (
                                                SELECT 1
                                                FROM sys.columns c
                                                WHERE c.object_id = OBJECT_ID(QUOTENAME(@schema)+N'.'+QUOTENAME(N'ProcessRevision'))
                                                  AND LOWER(c.name) = N'subtype'
                                            )
                                            BEGIN
                                                SET @sql = N'ALTER TABLE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(N'ProcessRevision')
                                                         + N' ADD ' + QUOTENAME(N'subType') + N' NVARCHAR(100) NULL;';
                                                EXEC sp_executesql @sql;
                                            END
                                        END

                                        -- ===== ProductRevision =====
                                        SET @schema = NULL;
                                        SELECT TOP(1) @schema = s.name
                                        FROM sys.tables t
                                        JOIN sys.schemas s ON s.schema_id = t.schema_id
                                        WHERE t.name = N'ProductRevision';

                                        IF @schema IS NOT NULL
                                        BEGIN
                                            IF NOT EXISTS (
                                                SELECT 1
                                                FROM sys.columns c
                                                WHERE c.object_id = OBJECT_ID(QUOTENAME(@schema)+N'.'+QUOTENAME(N'ProductRevision'))
                                                  AND LOWER(c.name) = N'subtype'
                                            )
                                            BEGIN
                                                SET @sql = N'ALTER TABLE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(N'ProductRevision')
                                                         + N' ADD ' + QUOTENAME(N'subType') + N' NVARCHAR(100) NULL;';
                                                EXEC sp_executesql @sql;
                                            END
                                        END

                                        COMMIT;
                                        ";



            string query = @"WITH FirstProcessName AS(
                            SELECT RIGHT(catalogueId,6) first_process_name
                          FROM Process p
                          INNER JOIN ProcessRevision pr ON pr.masterRef = p.id_Table
                          INNER JOIN ProcessOccurrence po ON pr.id_Table = po.instancedRef
                          WHERE po.parentRef IS NULL
                        )

            SELECT p.catalogueId AS codigo, 
            pr.revision AS revEstruct,
            CONCAT('Proceso: ', p.catalogueId, ' - ', fpn.first_process_name) AS descripcion,
            1 AS Cantidad,
            'PA' AS tipo,
            '01' AS deposito,
            'UN' AS 'Unidad de Medida',
            fpn.first_process_name AS Process_name,
            pr.subType
            FROM Operation O
            CROSS JOIN FirstProcessName fpn
            INNER JOIN OperationRevision OpR ON OpR.masterRef = o.id_Table
            INNER JOIN ProcessOccurrence po ON po.instancedRef = OpR.id_Table
            --INNER JOIN Form f ON f.name = CONCAT(o.catalogueId, '/', OpR.revision)
            CROSS APPLY (
              SELECT CASE
                       WHEN RIGHT(o.catalogueId, 3) = '-OP'
                            THEN LEFT(o.catalogueId, LEN(o.catalogueId) - 3)  -- quita ""-OP""
                       WHEN CHARINDEX('-OP', o.catalogueId) > 0
                            THEN REPLACE(o.catalogueId, '-OP', '')             -- más defensivo
                       ELSE o.catalogueId
                     END AS op_code_base
            ) AS x
			LEFT JOIN Form f ON f.name = CONCAT(x.op_code_base, '/', OpR.revision)
            INNER JOIN Form f2 ON f2.id_Table = f.id_Table + 3
            INNER JOIN UserValue_UserData uud ON uud.id_Father = f2.id_Table + 1 AND uud.title = 'allocated_time'
            INNER JOIN ProcessOccurrence po2 ON po2.id_Table = po.parentRef
            INNER JOIN ProcessRevision pr ON pr.id_Table = po2.instancedRef
            INNER JOIN Process p ON p.id_Table = pr.masterRef
            LEFT JOIN(SELECT p.catalogueId, sq1.productId FROM Process p
            INNER JOIN ProcessRevision pr ON pr.masterRef = p.id_Table
            INNER JOIN ProcessOccurrence po ON po.instancedRef = pr.id_Table
            INNER JOIN Occurrence o ON po.id_Table = o.parentRef
            INNER JOIN (SELECT p.productId, o.parentRef FROM Product p
            INNER JOIN ProductRevision pr ON pr.masterRef = p.id_Table
            INNER JOIN Occurrence o ON o.instancedRef = pr.id_Table
            WHERE o.subType IS NULL

            UNION ALL

            SELECT wa.catalogueId, wao.parentRef FROM WorkArea wa
            INNER JOIN WorkAreaRevision war ON war.masterRef = wa.id_Table
            INNER JOIN Occurrence wao ON wao.instancedRef = war.id_Table) sq1 ON sq1.parentRef = o.parentRef) sq2 ON sq2.catalogueId = p.catalogueId

            UNION ALL

            SELECT productId, pr.revision, pr.name, COUNT(productId) AS Cantidad,
            'PA' AS tipo,
            '01' AS deposito,
            'UN' AS unMedida,
            fpn.first_process_name AS Process_name,
            pr.subType
            FROM Product p
            INNER JOIN ProductRevision pr ON pr.masterRef = p.id_Table
            INNER JOIN Occurrence o ON o.instancedRef = pr.id_Table
            CROSS JOIN FirstProcessName fpn
            WHERE o.subType = 'MEConsumed'
            GROUP BY productId, pr.revision, pr.name, fpn.first_process_name, pr.subType";


            List<string> jsonProductos = new List<string>();

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    //Aseguro el schema con subType
                    using (var patchCmd = new SqlCommand(schemaPatch, connection))
                    {
                        patchCmd.ExecuteNonQuery();
                    }

                    using (SqlCommand command = new SqlCommand(query, connection))
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            // Construir el JSON para cada producto
                            var producto = new
                            {
                                producto = new List<Dictionary<string, string>>
                        {
                            new Dictionary<string, string> { { "campo", "codigo" }, { "valor", reader["codigo"].ToString() } },
                            new Dictionary<string, string> { { "campo", "descripcion" }, { "valor", reader["descripcion"].ToString() } },
                            new Dictionary<string, string> { { "campo", "tipo" }, { "valor", reader["tipo"].ToString() } },
                            new Dictionary<string, string> { { "campo", "deposito" }, { "valor", reader["deposito"].ToString() } },
                            new Dictionary<string, string> { { "campo", "unMedida" }, { "valor", reader["Unidad de Medida"].ToString() } }
                        }
                            };

                            string jsonData = JsonConvert.SerializeObject(producto, Formatting.Indented);
                            Console.WriteLine(jsonData);
                            jsonProductos.Add(jsonData); // Guardar el JSON en la lista
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al consultar la base de datos: {ex.Message}");
            }
            return jsonProductos;
        }
        public class Procedimiento
        {
            public List<CampoValor> detalle { get; set; }
        }

        public class CampoValor
        {
            public string campo { get; set; }
            public string valor { get; set; }
        }

    }
}
