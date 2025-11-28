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
using static Web_Service.Program;
using static Web_Service.Tablas_SG2_SH3;

namespace Web_Service
{
    public class Tablas_SG2_SH3
    {

        public sealed class WsError
        {
            public int? errorCode { get; set; }
            public string errorMessage { get; set; }
        }
        //public static async Task postSG2_SH3(string jsonData)
        //{
        //    string url = "http://119.8.73.193:8086/rest/TCProceso/Incluir/"; // URL del servicio REST

        //    string username = "USERREST"; // Usuario proporcionado
        //    string password = "restagr"; // Contraseña proporcionada


        //    using (HttpClient client = new HttpClient())
        //    {
        //        try
        //        {
        //            // Configurar credenciales Basic Auth
        //            var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
        //            client.DefaultRequestHeaders.Authorization =
        //                new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

        //            // Configurar el contenido de la solicitud
        //            var content = new StringContent(jsonData, Encoding.UTF8, "application/json");

        //            // Realizar la solicitud POST
        //            HttpResponseMessage response = await client.PostAsync(url, content);

        //            // Leer la respuesta como string
        //            string responseData = await response.Content.ReadAsStringAsync();

        //            Console.WriteLine($"POST TCProceso -> {(int)response.StatusCode} {response.ReasonPhrase}\n{responseData}");

        //            if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
        //            {
        //                Console.WriteLine("Ya existe (409). Intentando PUT /Modificar...");
        //                await putSG2_SH3(jsonData);     // llamada a la función PUT
        //                return;                         // salimos después del PUT
        //            }

        //            // Si no fue 409, ahora sí validamos éxito del POST
        //            //if (!response.IsSuccessStatusCode)
        //            //{
        //            //    //    var bodyErr = await response.Content.ReadAsStringAsync();
        //            //    Console.WriteLine($"POST falló (no 409). Ver detalle arriba.");
        //            //    return;
        //            //}
        //            //else
        //            //{
        //            //    Console.WriteLine("POST ok.");
        //            //}

        //            // Asegurarse de que la respuesta sea exitosa
        //            response.EnsureSuccessStatusCode();

        //            // Mostrar la respuesta en consola
        //            Console.WriteLine("Respuesta del servicio:");
        //            Console.WriteLine(responseData);
        //        }
        //        catch (Exception ex)
        //        {
        //            Console.WriteLine($"Error al consumir el servicio: {ex.Message}");
        //        }
        //    }
        //}



        //public static async Task putSG2_SH3(string jsonData)
        //{
        //    string url = "http://119.8.73.193:8086/rest/TCProceso/Modificar/";
        //    string username = "USERREST";
        //    string password = "restagr";

        //    using (var client = new HttpClient())
        //    {
        //        try
        //        {
        //            var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
        //            client.DefaultRequestHeaders.Authorization =
        //                new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

        //            var content = new StringContent(jsonData, Encoding.UTF8, "application/json");

        //            HttpResponseMessage response = await client.PutAsync(url, content);

        //            if (!response.IsSuccessStatusCode)
        //            {
        //                var bodyErr = await response.Content.ReadAsStringAsync();
        //                Console.WriteLine($"PUT falló: {(int)response.StatusCode} {response.ReasonPhrase}\n{bodyErr}");
        //                return;
        //            }

        //            string responseData = await response.Content.ReadAsStringAsync();
        //            Console.WriteLine("Respuesta del servicio (PUT ok):");
        //            Console.WriteLine(responseData);
        //            return;
        //        }
        //        catch (Exception ex)
        //        {
        //            Console.WriteLine($"Error al consumir el servicio: {ex.Message}");
        //        }
        //    }
        //}
        public static async Task EnviarSG2_SH3(string jsonData)
        {
            string urlPost = "http://119.8.73.193:8086/rest/TCProceso/Incluir/";
            string urlPut = "http://119.8.73.193:8086/rest/TCProceso/Modificar/";
            string username = "USERREST";
            string password = "restagr";

            using var client = new HttpClient();
            var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
            client.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

            // 1) Intento POST
            var contentPost = new StringContent(jsonData, Encoding.UTF8, "application/json");
            var responsePost = await client.PostAsync(urlPost, contentPost);
            var bodyPost = await responsePost.Content.ReadAsStringAsync();

            Console.WriteLine($"POST TCProceso -> {(int)responsePost.StatusCode} {responsePost.ReasonPhrase}");
            Console.WriteLine(bodyPost);

            if (responsePost.IsSuccessStatusCode)
            {
                Console.WriteLine("POST OK, no hace falta PUT.");
                return;
            }

            // 2) Solo si es un caso de "ya existe", hago UN SOLO PUT
            bool esRegistroExistente =
                responsePost.StatusCode == System.Net.HttpStatusCode.Conflict ||
                bodyPost.Contains("ya existe", StringComparison.OrdinalIgnoreCase) ||
                bodyPost.Contains("Registro duplicado", StringComparison.OrdinalIgnoreCase);

            if (!esRegistroExistente)
            {
                Console.WriteLine("POST falló por otra causa, NO se intenta PUT.");
                return;
            }

            Console.WriteLine("Intentando PUT /Modificar por registro existente...");

            var contentPut = new StringContent(jsonData, Encoding.UTF8, "application/json");
            var responsePut = await client.PutAsync(urlPut, contentPut);
            var bodyPut = await responsePut.Content.ReadAsStringAsync();

            Console.WriteLine($"PUT TCProceso -> {(int)responsePut.StatusCode} {responsePut.ReasonPhrase}");
            Console.WriteLine(bodyPut);

            if (!responsePut.IsSuccessStatusCode)
            {
                Console.WriteLine("PUT falló, no se reintenta para evitar bucles.");
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

        public static string NormalizarCentroTrabajo(string centroTrabajo)
        {
            if (string.IsNullOrWhiteSpace(centroTrabajo))
                return "000";  // valor por defecto si viene vacío

            // Dejar solo dígitos (por si viniera con espacios o caracteres)
            centroTrabajo = new string(centroTrabajo.Where(char.IsDigit).ToArray());

            if (centroTrabajo.Length <= 3)
                return centroTrabajo.PadLeft(3, '0');  // si tiene menos de 3 dígitos, completamos

            return centroTrabajo.Substring(centroTrabajo.Length - 3); // últimos 3 dígitos
        }


        //Consulta con Workarea y recurso hijo
        private const string consultaD_workarea_recurso = @"SELECT
    p.catalogueId   AS Padre,
    p.name          AS descripcion,
    wa.catalogueId  AS centroTrabajo,
    prod.productId  AS recurso, 
    op.catalogueId  AS Operacion,
    ta.tiempo_segundos,
    ta.tiempo_fmt   AS tiempo,
    ta.lote_val     AS loteStd,
	ROW_NUMBER() OVER (
        PARTITION BY p.catalogueId, op.catalogueId, wa.catalogueId
        ORDER BY prod.productId
    ) AS nro_recurso
FROM ProcessRevision        AS pr
INNER JOIN Process                AS p   ON p.id_Table  = pr.masterRef
INNER JOIN ProcessOccurrence      AS po  ON po.instancedRef = pr.id_Table

-- WorkArea
INNER JOIN Occurrence             AS occ1 
        ON occ1.parentRef = po.id_Table
       AND occ1.subType   = 'MEWorkArea'
INNER JOIN WorkAreaRevision       AS war ON war.id_Table    = occ1.instancedRef
INNER JOIN WorkArea               AS wa  ON wa.id_Table     = war.masterRef

-- Recursos hijos de la WorkArea
INNER JOIN WorkAreaOccurrence wao ON wao.instancedRef = war.id_Table
INNER JOIN Occurrence occ2        ON occ2.parentRef   = wao.id_Table
INNER JOIN ProductRevision prod_rev ON prod_rev.id_Table = occ2.instancedRef
INNER JOIN Product prod             ON prod.id_Table     = prod_rev.masterRef

-- Operaciones
LEFT JOIN ProcessOccurrence po_op 
        ON po_op.parentRef = po.id_Table
LEFT JOIN OperationRevision op_rev 
        ON op_rev.id_Table = po_op.instancedRef
LEFT JOIN Operation op 
        ON op.id_Table = op_rev.masterRef

OUTER APPLY (
    SELECT TOP 1
        uvud_time.value AS tiempo_segundos,
        CASE
            WHEN x2.t = 0.0 THEN '0.01'
            WHEN x2.t < 60 THEN REPLACE(CONVERT(varchar(5), DATEADD(SECOND, x2.t * 60, 0), 108), ':', '.')
            ELSE REPLACE(CONVERT(varchar(5), DATEADD(SECOND, x2.t, 0), 108), ':', '.')
        END AS tiempo_fmt,
        CASE WHEN x2.t < 60 THEN 60 ELSE 1 END AS lote_val
    FROM STRING_SPLIT(po_op.associatedAttachmentRefs, ' ') s
    JOIN AssociatedAttachment aa
       ON aa.id_Table = RIGHT(s.value, LEN(s.value) - 3)
    JOIN Form f_time
       ON f_time.id_Table = RIGHT(aa.attachmentRef, LEN(aa.attachmentRef) - 3)
      AND f_time.subType = 'MEOpTimeAnalysis'
    JOIN UserValue_UserData uvud_time
       ON uvud_time.id_Father = f_time.id_Table + 1
      AND uvud_time.title = 'allocated_time'
    CROSS APPLY (
        SELECT t = COALESCE(TRY_CAST(uvud_time.value AS float), 0.0)
    ) AS x2
) AS ta

ORDER BY RIGHT(p.catalogueId, LEN(p.catalogueId) - 3) DESC;";
        //Consulta para los xml que vienen sin WorkAreaOccurrence
        private const string ConsultaA_ConWorkArea_SinWAO = @"
SELECT
    p.catalogueId   AS Padre,
    p.name          AS descripcion,
    wa.catalogueId  AS centroTrabajo,
    prod.productId  AS recurso, 
    op.catalogueId  AS Operacion,
    ta.tiempo_segundos,
    ta.tiempo_fmt   AS tiempo,
    ta.lote_val     AS loteStd,
    ROW_NUMBER() OVER (
        PARTITION BY p.catalogueId, op.catalogueId, wa.catalogueId
        ORDER BY prod.productId
    ) AS nro_recurso
FROM ProcessRevision        AS pr
INNER JOIN Process                AS p   ON p.id_Table  = pr.masterRef
INNER JOIN ProcessOccurrence      AS po  ON po.instancedRef = pr.id_Table

-- WorkArea hija del PR
INNER JOIN Occurrence             AS occ1 
        ON occ1.parentRef = po.id_Table
       AND occ1.subType   = 'MEWorkArea'
INNER JOIN WorkAreaRevision       AS war ON war.id_Table    = occ1.instancedRef
INNER JOIN WorkArea               AS wa  ON wa.id_Table     = war.masterRef

-- Recursos: hijos de la WorkArea sin usar WorkAreaOccurrence
INNER JOIN Occurrence occ2        
        ON occ2.parentRef   = occ1.id_Table
INNER JOIN ProductRevision prod_rev 
        ON prod_rev.id_Table = occ2.instancedRef
INNER JOIN Product prod           
        ON prod.id_Table     = prod_rev.masterRef

-- Operaciones
LEFT JOIN ProcessOccurrence po_op 
        ON po_op.parentRef = po.id_Table
LEFT JOIN OperationRevision op_rev 
        ON op_rev.id_Table = po_op.instancedRef
LEFT JOIN Operation op 
        ON op.id_Table = op_rev.masterRef

-- Tiempo por operación
OUTER APPLY (
    SELECT TOP 1
        uvud_time.value AS tiempo_segundos,
        CASE
            WHEN x2.t = 0.0 THEN '0.01'
            WHEN x2.t < 60 THEN REPLACE(CONVERT(varchar(5), DATEADD(SECOND, x2.t * 60, 0), 108), ':', '.')
            ELSE REPLACE(CONVERT(varchar(5), DATEADD(SECOND, x2.t, 0), 108), ':', '.')
        END AS tiempo_fmt,
        CASE WHEN x2.t < 60 THEN 60 ELSE 1 END AS lote_val
    FROM STRING_SPLIT(po_op.associatedAttachmentRefs, ' ') s
    JOIN AssociatedAttachment aa
       ON aa.id_Table = RIGHT(s.value, LEN(s.value) - 3)
    JOIN Form f_time
       ON f_time.id_Table = RIGHT(aa.attachmentRef, LEN(aa.attachmentRef) - 3)
      AND f_time.subType = 'MEOpTimeAnalysis'
    JOIN UserValue_UserData uvud_time
       ON uvud_time.id_Father = f_time.id_Table + 1
      AND uvud_time.title = 'allocated_time'
    CROSS APPLY (
        SELECT t = COALESCE(TRY_CAST(uvud_time.value AS float), 0.0)
    ) AS x2
) AS ta

ORDER BY RIGHT(p.catalogueId, LEN(p.catalogueId) - 3) DESC;
";
        //Consulta para los xml que vienen sin WorkArea
        private const string consultaB_sin_workarea = @"SELECT
    p.catalogueId   AS Padre,
    p.name          AS descripcion,
    NULL            AS centroTrabajo,          -- no hay WA
    prod.productId  AS recurso,          -- recurso real

    uud.value               AS tiempo_segundos,
    calc.tiempo_fmt         AS tiempo,
    calc.lote_val           AS loteStd,
    --pr_ins.sequenceNumber   AS nro_busqueda,
    op.catalogueId          AS Operacion,
    ROW_NUMBER() OVER (PARTITION BY p.catalogueId ORDER BY op.catalogueId) AS nro_recurso

FROM ProcessRevision        AS pr
JOIN Process                AS p
      ON p.id_Table = pr.masterRef

-- Proceso / subproceso
JOIN ProcessOccurrence      AS po
      ON po.instancedRef = pr.id_Table

-- Recursos: hijos directos del PR (sin WorkArea)
JOIN Occurrence             AS occ2
      ON occ2.parentRef = po.id_Table

JOIN ProductRevision        AS prod_rev
      ON prod_rev.id_Table = occ2.instancedRef

JOIN Product                AS prod
      ON prod.id_Table = prod_rev.masterRef

-- Form maestro de proceso + TimeAnalysis
LEFT JOIN Form AS f_proc
       ON f_proc.name = CONCAT(p.catalogueId, '/', pr.revision)

LEFT JOIN Form AS f_time
       ON f_time.id_Table = f_proc.id_Table + 3

LEFT JOIN UserValue_UserData AS uud
       ON uud.id_Father = f_time.id_Table + 1
      AND uud.title = 'allocated_time'

-- Vista e instancia de proceso (para nro_busqueda)
INNER JOIN ProcessRevisionView pr_view 
        ON RIGHT(pr_view.revisionRef, LEN(pr_view.revisionRef) - 3) = pr.id_Table

INNER JOIN ProcessInstance pr_ins 
        ON RIGHT(pr_ins.partRef, LEN(pr_ins.partRef) - 3) = pr_view.id_Table

-- Operaciones
INNER JOIN ProcessOccurrence po_op 
        ON po_op.parentRef = po.id_Table

INNER JOIN OperationRevision op_rev 
        ON op_rev.id_Table = po_op.instancedRef

INNER JOIN Operation op 
        ON op.id_Table = op_rev.masterRef

-- Cálculo de tiempo
CROSS APPLY (
    SELECT t = COALESCE(TRY_CAST(uud.value AS float), 0.0)
) AS x2
CROSS APPLY (
    SELECT
        tiempo_fmt = CASE
            WHEN x2.t = 0.0 THEN '0.01'
            WHEN x2.t < 60 THEN REPLACE(CONVERT(varchar(5), DATEADD(SECOND, x2.t * 60, 0), 108), ':', '.')
            ELSE REPLACE(CONVERT(varchar(5), DATEADD(SECOND, x2.t, 0), 108), ':', '.')
        END,
        lote_val = CASE WHEN x2.t < 60 THEN 60 ELSE 1 END
) AS calc

-- Solo filas que tengan recurso
--WHERE prod.productId IS NOT NULL

ORDER BY RIGHT(p.catalogueId, LEN(p.catalogueId) - 3) DESC;";

        //Consulta para los xml que vienen con WorkArea que apunta directo a ProductRevision
        private const string ConsultaC_WorkAreaEspecial = @"
-- MEWorkArea que apunta directo a ProductRevision

SELECT
    p.catalogueId   AS Padre,
    p.name          AS descripcion,
    NULL            AS centroTrabajo,
    wa_inst.productId  AS recurso,     -- la máquina/estación como recurso
    op.catalogueId  AS Operacion,
    ta.tiempo_segundos,
    ta.tiempo_fmt   AS tiempo,
    ta.lote_val     AS loteStd

FROM ProcessRevision pr
JOIN Process p 
       ON p.id_Table = pr.masterRef
JOIN ProcessOccurrence po
       ON po.instancedRef = pr.id_Table

-- MEWorkArea -> ProductRevision
JOIN Occurrence occ_wa
       ON occ_wa.parentRef = po.id_Table
      AND occ_wa.subType   = 'MEWorkArea'
JOIN ProductRevision prod_rev
       ON prod_rev.id_Table = occ_wa.instancedRef
JOIN Product wa_inst
       ON wa_inst.id_Table = prod_rev.masterRef

-- Operaciones
LEFT JOIN ProcessOccurrence po_op 
       ON po_op.parentRef = po.id_Table
LEFT JOIN OperationRevision op_rev 
       ON op_rev.id_Table = po_op.instancedRef
LEFT JOIN Operation op 
       ON op.id_Table = op_rev.masterRef

-- Tiempo por operación
OUTER APPLY (
    SELECT TOP 1
        uvud_time.value AS tiempo_segundos,
        CASE
            WHEN x2.t = 0.0 THEN '0.01'
            WHEN x2.t < 60 THEN REPLACE(CONVERT(varchar(5), DATEADD(SECOND, x2.t * 60, 0), 108), ':', '.')
            ELSE REPLACE(CONVERT(varchar(5), DATEADD(SECOND, x2.t, 0), 108), ':', '.')
        END AS tiempo_fmt,
        CASE WHEN x2.t < 60 THEN 60 ELSE 1 END AS lote_val
    FROM STRING_SPLIT(po_op.associatedAttachmentRefs, ' ') s
    JOIN AssociatedAttachment aa
        ON aa.id_Table = RIGHT(s.value, LEN(s.value) - 3)
    JOIN Form f_time
        ON f_time.id_Table = RIGHT(aa.attachmentRef, LEN(aa.attachmentRef) - 3)
       AND f_time.subType = 'MEOpTimeAnalysis'
    JOIN UserValue_UserData uvud_time
        ON uvud_time.id_Father = f_time.id_Table + 1
       AND uvud_time.title = 'allocated_time'
    CROSS APPLY (
        SELECT t = COALESCE(TRY_CAST(uvud_time.value AS float), 0.0)
    ) AS x2
) AS ta

ORDER BY RIGHT(p.catalogueId, LEN(p.catalogueId) - 3) DESC;
";


        public static string ObtenerConsultaRecursos(SqlConnection connection)
        {
            bool hayWorkArea = false;
            bool esWorkAreaNormal = false;
            bool esWorkAreaEspecial = false;
            bool existeTablaWAO = false;

            // ¿Existe físicamente la tabla WorkArea?
            // Si existe, este XML tenía nodo <WorkArea>
            using (var cmd = new SqlCommand(
                "SELECT OBJECT_ID('WorkArea', 'U');", connection))
            {
                var res = cmd.ExecuteScalar();
                hayWorkArea = (res != null && res != DBNull.Value);
            }

            if (hayWorkArea)
            {
                // Caso A: MEWorkArea -> WorkAreaRevision
                using (var cmd = new SqlCommand(@"
            SELECT TOP 1 1
            FROM Occurrence o
            JOIN WorkAreaRevision war ON war.id_Table = o.instancedRef
            WHERE o.subType = 'MEWorkArea';", connection))
                {
                    var res = cmd.ExecuteScalar();
                    esWorkAreaNormal = (res != null && res != DBNull.Value);
                }

                // Caso C: MEWorkArea -> ProductRevision directo
                using (var cmd = new SqlCommand(@"
            SELECT TOP 1 1
            FROM Occurrence o
            JOIN ProductRevision pr ON pr.id_Table = o.instancedRef
            WHERE o.subType = 'MEWorkArea';", connection))
                {
                    var res = cmd.ExecuteScalar();
                    esWorkAreaEspecial = (res != null && res != DBNull.Value);
                }

                // ¿Existe físicamente la tabla WorkAreaOccurrence?
                using (var cmd = new SqlCommand(
                    "SELECT OBJECT_ID('WorkAreaOccurrence', 'U');", connection))
                {
                    var res = cmd.ExecuteScalar();
                    existeTablaWAO = (res != null && res != DBNull.Value);
                }
            }

            Utilidades.EscribirEnLog(
                "ObtenerQueryRecursos -> hayWorkArea=" + hayWorkArea +
                ", normal=" + esWorkAreaNormal +
                ", especial=" + esWorkAreaEspecial +
                ", existeWAO=" + existeTablaWAO);

            Utilidades.EscribirEnLog("Query elegida: " +
                (!hayWorkArea ? "B" :
                 esWorkAreaNormal && existeTablaWAO ? "A_WAO" :
                 esWorkAreaNormal && !existeTablaWAO ? "A_SinWAO" :
                 esWorkAreaEspecial ? "C" : "NINGUNA"));

            // 🔀 Selección de consulta según combinación detectada

            if (!hayWorkArea)
                return consultaB_sin_workarea;              // sin WorkArea

            if (esWorkAreaNormal)
            {
                if (existeTablaWAO)
                    return consultaD_workarea_recurso;      // usa WorkAreaOccurrence (A_WAO)
                else
                    return ConsultaA_ConWorkArea_SinWAO;   // sin WorkAreaOccurrence (A_SinWAO)
            }

            if (esWorkAreaEspecial)
                return ConsultaC_WorkAreaEspecial;         // MEWorkArea -> ProductRevision (C)

            throw new Exception("No se pudo determinar el tipo de WorkArea para este XML.");
        }



        public static List<string> jsonSG2_SH3()
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                      Integrated Security=True;TrustServerCertificate=True";


            List<string> jsonProductos = new List<string>();
            Utilidades.EscribirEnLog("jsonSG2_SH3 -> entrando al método");

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    Utilidades.EscribirEnLog("jsonSG2_SH3 -> conexión abierta");
                    string query = ObtenerConsultaRecursos(connection);
                    Utilidades.EscribirEnLog("jsonSG2_SH3 -> query elegida:\n" + query);
                    SqlCommand command = new SqlCommand(query, connection);
                    SqlDataReader reader = command.ExecuteReader();

                    int filas = 0;
                    Dictionary<string, dynamic> productosDict = new Dictionary<string, dynamic>();
                    //Dictionary<string, Dictionary<string, int>> productoRecursoOperacion = new Dictionary<string, Dictionary<string, int>>();
                    var procPorProductoOperacion = new Dictionary<string, Procedimiento>();
                    var ultimoPasoPorProducto = new Dictionary<string, int>();

                    while (reader.Read())
                    {
                        filas++;
                        // DEBUG: logueamos la primera fila para ver qué trae
                        if (filas <= 3)
                        {
                            Utilidades.EscribirEnLog(
                                $"jsonSG2_SH3 -> fila {filas}: Padre={reader["Padre"]}, recurso={reader["recurso"]}, op={reader["Operacion"]}, tiempo={reader["tiempo"]}");
                        }
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
                        string centroTrabajo = reader["centroTrabajo"]?.ToString();
                        centroTrabajo = NormalizarCentroTrabajo(centroTrabajo);
                        string operacion = reader["Operacion"]?.ToString();

                        int nroRecurso = Convert.ToInt32(reader["nro_recurso"]);                     

                         // Asignar operacion (incrementa por recurso)
                        //if (!productoRecursoOperacion.ContainsKey(producto))
                        //{
                        //    productoRecursoOperacion[producto] = new Dictionary<string, int>();
                        //}

                        //int operacionActual = 10;
                        //if (productoRecursoOperacion[producto].ContainsKey(recurso))
                        //{
                        //    operacionActual = productoRecursoOperacion[producto][recurso] + 10;
                        //}
                        //productoRecursoOperacion[producto][recurso] = operacionActual;

                        /*string operacion = operacionActual.ToString("D2");*/ // Formato 0010, 0020, etc.

                        //clave unica para cada op
                        string keyProc = producto + "_" + operacion;

                        if (!procPorProductoOperacion.ContainsKey(keyProc))
                        {
                            int operacionActual = 10;
                            if (ultimoPasoPorProducto.ContainsKey(producto))
                            {
                                operacionActual = ultimoPasoPorProducto[producto] + 10;
                            }
                            ultimoPasoPorProducto[producto] = operacionActual;
                            string operacionPaso = operacionActual.ToString("D2");

                            var nuevoProc = new Procedimiento
                            {
                                detalle = new List<CampoValor>
                                {
                                    new CampoValor { campo = "operacion",   valor = operacionPaso },
                                    new CampoValor { campo = "recurso",     valor = "" },  // se completa con el principal
                                    new CampoValor { campo = "tiempo",      valor = tiempo },
                                    new CampoValor { campo = "centroTrabajo", valor = centroTrabajo },
                                    new CampoValor { campo = "descripcion", valor = nombreOperacion },
                                    new CampoValor { campo = "loteStd",     valor = lote }
                                },
                                alternativos = new List<CampoValor>()
                            };

                            procPorProductoOperacion[keyProc] = nuevoProc;

                            //if (!productosDict.ContainsKey(producto))
                            //{
                            //    productosDict[producto] = new
                            //    {
                            //        codigo = codigo,
                            //        producto = producto,
                            //        procedimiento = new List<Procedimiento>()
                            //    };
                            //}

                            //productosDict[producto].procedimiento.Add(nuevoProc);
                            if (productosDict.ContainsKey(producto))
                            {
                                productosDict[producto].procedimiento.Add(nuevoProc);
                            }
                            else
                            {
                                productosDict[producto] = new
                                {
                                    codigo = codigo,
                                    producto = producto,
                                    procedimiento = new List<Procedimiento> { nuevoProc }
                                };
                            }
                        }

                        Procedimiento proc = procPorProductoOperacion[keyProc];

                        if (nroRecurso == 1)
                        {
                            //Recurso principal
                            var campoRecurso = proc.detalle.First(x => x.campo == "recurso");
                            campoRecurso.valor = recurso;

                            var campoTiempo = proc.detalle.First(x => x.campo == "tiempo");
                            campoTiempo.valor = tiempo;
                        }
                        else
                        {
                            //Recurso alternativo
                            proc.alternativos.Add(new CampoValor
                            {
                                campo = "recursoAlt",
                                valor = recurso,
                                
                            });
                            proc.alternativos.Add(new CampoValor
                            {
                                campo = "tipoAlt",
                                valor = "A"
                            });
                        }

                       
                    }

                    Utilidades.EscribirEnLog($"jsonSG2_SH3 -> filas leídas de la query: {filas}");
                    Utilidades.EscribirEnLog($"jsonSG2_SH3 -> productosDict.Count = {productosDict.Count}");


                    foreach (var item in productosDict.Values)
                    {
                        string json = JsonConvert.SerializeObject(item, Formatting.Indented);
                        Console.WriteLine(json);
                        Utilidades.EscribirEnLog("jsonSG2_SH3 -> JSON generado:\n" + json);
                        jsonProductos.Add(json);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
                Utilidades.EscribirEnLog("Error en jsonSG2_SH3: " + ex.Message);
            }

            return jsonProductos;

        }



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



            string query = @"WITH FirstProcess_codigo AS(
                            SELECT RIGHT(catalogueId,6) first_process_name
                          FROM Process p
                          INNER JOIN ProcessRevision pr ON pr.masterRef = p.id_Table
                          INNER JOIN ProcessOccurrence po ON pr.id_Table = po.instancedRef
                          WHERE po.parentRef IS NULL
                        )

            SELECT p.catalogueId AS codigo, uud2.value,
            pr.revision AS revEstruct,
            CONCAT('Proceso: ', p.catalogueId, ' - ', fpn.first_process_name) AS descripcion,
            1 AS Cantidad,
            'PA' AS tipo,
            '01' AS deposito,
            'UN' AS 'Unidad de Medida',
            fpn.first_process_name AS Process_name,
            pr.subType
            FROM Operation O
            CROSS JOIN FirstProcess_codigo fpn
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
			LEFT JOIN UserValue_UserData uud2 ON uud2.id_Father = po2.id_Table + 2 AND uud2.title = 'SequenceNumber'
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

            SELECT productId, 0 as value, pr.revision, pr.name, COUNT(productId) AS Cantidad,
            'PA' AS tipo,
            '01' AS deposito,
            'UN' AS unMedida,
            fpn.first_process_name AS Process_name,
            pr.subType
            FROM Product p
            INNER JOIN ProductRevision pr ON pr.masterRef = p.id_Table
            INNER JOIN Occurrence o ON o.instancedRef = pr.id_Table
            CROSS JOIN FirstProcess_codigo fpn
            WHERE o.subType = 'MEConsumed' OR pr.subType LIKE '%MatPrima%'
            GROUP BY productId, pr.revision, pr.name, fpn.first_process_name, pr.subType
			ORDER BY uud2.value DESC, p.catalogueId DESC";


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
            public List<CampoValor> alternativos { get; set; }
        }

        public class CampoValor
        {
            public string campo { get; set; }
            public string valor { get; set; }
        }

    }
}
