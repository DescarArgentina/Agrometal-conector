using System;
using System.IO;
using System.Text;

namespace Web_Service
{
    public static class Utilidades
    {
        private static readonly object _lock = new();
        private static string _rutaLog = @"C:\Temp\log2212.txt"; // fallback si no se inicializa

        /// <summary>
        /// Inicializa el log para esta ejecución (1 archivo por MBOM).
        /// Debe llamarse una vez al inicio del Main con MBOM_INPUT.
        /// </summary>
        public static void InicializarLogPorMbom(string mbomFolderPath)
        {
            if (string.IsNullOrWhiteSpace(mbomFolderPath))
                return;

            Directory.CreateDirectory(mbomFolderPath);

            // Un único log por MBOM (por carpeta M-BOM_xxx)
            _rutaLog = Path.Combine(mbomFolderPath, "ScriptPrincipal.log");
        }

        public static void EscribirEnLog(string mensaje)
        {
            try
            {
                var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} - {mensaje}{Environment.NewLine}";
                lock (_lock)
                {
                    File.AppendAllText(_rutaLog, line, Encoding.UTF8);
                }
            }
            catch
            {
                // No abortar la ejecución por un fallo de log.
            }
        }
    }
}
