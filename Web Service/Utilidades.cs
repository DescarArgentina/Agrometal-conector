using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Web_Service
{
    public static class Utilidades
    {
        public static void EscribirEnLog(string mensaje)
        {
            // Lógica para escribir en el log
            string rutaLog = "C:\\Temp\\log.txt";
            //string rutaLog = @"C:\Crucianelli\log.txt";
            File.AppendAllText(rutaLog, $"{DateTime.Now} - {mensaje}{Environment.NewLine}");
        }
    }
}
