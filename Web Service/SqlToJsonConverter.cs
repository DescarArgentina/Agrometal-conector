namespace Web_Service
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Text.RegularExpressions;
    using Newtonsoft.Json;
    using System.Globalization;

    public class ProductStructure
    {
        public string producto { get; set; }
        public string qtdBase { get; set; }
        public List<List<Campo>> estructura { get; set; } = new List<List<Campo>>();
    }

    public class Campo
    {
        public string campo { get; set; }
        public string valor { get; set; }
    }

    public class SqlRecord
    {
        public string Process_codigo { get; set; }
        public string PRCodigo { get; set; }
        public string Codigo { get; set; }
        public double Cantidad { get; set; }
        public string Subtype { get; set; }  // <-- nuevo campo

        public string Nombre_WA { get; set; }
    }

    public class SqlToJsonConverter
    {
        private readonly string connectionString;

        public SqlToJsonConverter(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public List<ProductStructure> ConvertSqlToHierarchicalJsons(string sqlQuery)
        {
            var records = new List<SqlRecord>();

            // 1) Leer la consulta
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand(sqlQuery, conn))
                using (var rdr = cmd.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        records.Add(new SqlRecord
                        {
                            Process_codigo = rdr["Process_codigo"].ToString(),
                            PRCodigo = rdr["PR_Codigo"].ToString(),
                            Codigo = rdr["Codigo"].ToString(),
                            Cantidad = Convert.ToDouble(rdr["Cantidad"]),
                            Subtype = rdr["subType"].ToString()
                        });
                    }
                }
            }

            if (!records.Any())
                return new List<ProductStructure>();

            // 2) Obtener root (ej: 022116) desde Process_codigo
            var match = Regex.Match(records[0].Process_codigo ?? string.Empty, @"\d+");
            string rootParent = match.Success ? match.Value : records[0].Process_codigo;

            // 3) Lista de PRs distintos, en orden descendente (como ya viene la consulta)
            var prList = records
                .Select(r => r.PRCodigo)
                .Where(c => !string.IsNullOrWhiteSpace(c))
                .Distinct()
                .OrderByDescending(c => c)  // o simplemente .ToList() si ya viene en orden
                .ToList();

            // 4) Diccionario de ProductStructure por código
            var map = new Dictionary<string, ProductStructure>(StringComparer.OrdinalIgnoreCase);

            ProductStructure GetOrCreate(string codigo)
            {
                if (!map.TryGetValue(codigo, out var ps))
                {
                    ps = new ProductStructure
                    {
                        producto = codigo,
                        qtdBase = "1",
                        estructura = new List<List<Campo>>()
                    };
                    map[codigo] = ps;
                }
                return ps;
            }

            // 5) Construir la cadena: root → PR1 → PR2 → PR3
            //    y garantizar que cada PR tenga su ProductStructure vacío
            var rootPs = GetOrCreate(rootParent);

            for (int i = 0; i < prList.Count; i++)
            {
                string prCode = prList[i];
                string parent = (i == 0) ? rootParent : prList[i - 1];

                var psParent = GetOrCreate(parent);

                // Agregamos el PR como hijo del padre (root o PR anterior)
                psParent.estructura.Add(new List<Campo>
        {
            new Campo { campo = "codigo",   valor = prCode },
            new Campo { campo = "cantidad", valor = "1" }  // PR como intermedio, cantidad 1
        });

                // Aseguramos que el PR exista en el map (para luego colgarle consumibles)
                GetOrCreate(prCode);
            }

            // 6) Agregar los consumibles a cada PR según PR_Codigo
            foreach (var rec in records)
            {
                // Si por alguna razón el hijo tiene subtype de operación/herramental, podés filtrarlo aquí:
                var st = (rec.Subtype ?? string.Empty).ToLowerInvariant();
                if (st.Contains("operation") || st.Contains("meoperation") ||
                    st.Contains("fixture") || st.Contains("tool"))
                {
                    continue; // lo excluimos
                }

                var psParent = GetOrCreate(rec.PRCodigo);

                psParent.estructura.Add(new List<Campo>
        {
            new Campo { campo = "codigo",   valor = rec.Codigo },
            new Campo { campo = "cantidad", valor = rec.Cantidad.ToString(CultureInfo.InvariantCulture) }
        });
            }

            // 7) Armar la lista final de ProductStructure:
            //    primero root, luego PRs en el orden deseado.
            var result = new List<ProductStructure>();

            if (map.ContainsKey(rootParent))
                result.Add(map[rootParent]);

            foreach (var pr in prList)
            {
                if (map.TryGetValue(pr, out var ps))
                    result.Add(ps);
            }

            return result;
        }


        public List<string> ConvertToHierarchicalJsonStrings(string sqlQuery)
        {
            var products = ConvertSqlToHierarchicalJsons(sqlQuery);
            return products
                .Select(p => JsonConvert.SerializeObject(p, Formatting.Indented))
                .ToList();
        }

        // Método para mostrar la estructura jerárquica
        private void PrintProductStructure(
            string codigo,
            Dictionary<string, ProductStructure> map,
            string indent = "",
            HashSet<string> visited = null)
        {
            visited ??= new HashSet<string>();

            // Evitar ciclos (por si alguna vez aparece)
            if (visited.Contains(codigo))
            {
                Console.WriteLine($"{indent}- {codigo}  (loop detectado)");
                return;
            }

            visited.Add(codigo);

            if (!map.TryGetValue(codigo, out var product))
            {
                Console.WriteLine($"{indent}- {codigo}  (sin estructura)");
                return;
            }

            Console.WriteLine($"{indent}{codigo}");

            // Si no tiene estructura, no seguimos
            if (product.estructura == null || product.estructura.Count == 0)
                return;

            foreach (var relacion in product.estructura)
            {
                // Cada relacion es una lista de Campos
                var childCode = relacion.First(c => c.campo == "codigo").valor;

                Console.WriteLine($"{indent}  ├─ {childCode}");

                // Si ese hijo también es padre → imprimimos recursivamente
                if (map.ContainsKey(childCode))
                    PrintProductStructure(childCode, map, indent + "  │   ", visited);
            }
        }

       

        // Método para guardar cada JSON en archivos separados con nombres descriptivos
        public void SaveHierarchicalJsonFiles(string sqlQuery, string basePath = "")
        {
            var products = ConvertSqlToHierarchicalJsons(sqlQuery);

            for (int i = 0; i < products.Count; i++)
            {
                var product = products[i];

                // Extraemos hasta 3 hijos (si tiene)
                var hijos = product.estructura?
                    .Select(rel => rel.First(c => c.campo == "codigo").valor)
                    .Take(3)
                    .ToList() ?? new List<string>();

                string hijosStr;

                if (hijos.Count == 0)
                {
                    hijosStr = "sin_hijos";
                }
                else
                {
                    // Unir hijos usando guiones bajos
                    hijosStr = string.Join("_", hijos);
                }

                // Armar el JSON
                string json = JsonConvert.SerializeObject(product, Formatting.Indented);

                // Construir nombre descriptivo
                string fileName =
                    $"{basePath}relacion_{i + 1:D3}_padre_{product.producto}_hijos_{hijosStr}.json";

                // Guardar archivo
                System.IO.File.WriteAllText(fileName, json);

                Console.WriteLine($"Guardado: {fileName}");
            }
        }

       

        // Método para procesar y mostrar cada JSON individual
        public void ProcessHierarchicalJsons(string sqlQuery)
        {
            var products = ConvertSqlToHierarchicalJsons(sqlQuery);

            Console.WriteLine($"Total de JSONs jerárquicos generados: {products.Count}");
            Console.WriteLine(new string('=', 50));

            for (int i = 0; i < products.Count; i++)
            {
                var product = products[i];
                string json = JsonConvert.SerializeObject(product, Formatting.Indented);

                Console.WriteLine($"JSON #{i + 1}:");
                Console.WriteLine(json);
                Console.WriteLine(new string('-', 30));
            }
        }

        // Método para generar un resumen de la cadena jerárquica
        public void ShowBOMTree(string sqlQuery)
        {
            Console.WriteLine("ÁRBOL COMPLETO DE LA ESTRUCTURA BOM");
            Console.WriteLine("====================================");

            // Obtenemos la estructura jerárquica
            var products = ConvertSqlToHierarchicalJsons(sqlQuery);

            // Convertimos la lista en un diccionario para acceso rápido
            var map = products.ToDictionary(p => p.producto, p => p);

            // El primero siempre es el root (ej: 22116)
            var root = products[0].producto;

            PrintProductStructure(root, map);
        }

    }
}


