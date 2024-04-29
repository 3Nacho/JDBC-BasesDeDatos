package lsi.ubu.tests;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.excepciones.CompraBilleteTrenException;
import lsi.ubu.servicios.Servicio;
import lsi.ubu.servicios.ServicioImpl;
import lsi.ubu.util.PoolDeConexiones;

public class Tests {

	/** Logger. */
	private static final Logger LOGGER = LoggerFactory.getLogger(Tests.class);

	public static final String ORIGEN = "Burgos";
	public static final String DESTINO = "Madrid";

	public void ejecutarTestsCompraBilletes() {

		Servicio servicio = new ServicioImpl();

		PoolDeConexiones pool = PoolDeConexiones.getInstance();

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;

		// Prueba caso no existe el viaje
		try {
			java.util.Date fecha = toDate("15/04/2010");
			Time hora = Time.valueOf("12:00:00");
			int nroPlazas = 3;

			servicio.comprarBillete(hora, fecha, ORIGEN, DESTINO, nroPlazas);

			LOGGER.info("NO se da cuenta de que no existe el viaje MAL");
		} catch (SQLException e) {
			if (e.getErrorCode() == CompraBilleteTrenException.NO_EXISTE_VIAJE) {
				LOGGER.info("Se da cuenta de que no existe el viaje OK");
			}
		}

		// Prueba caso si existe pero no hay plazas
		try {
			java.util.Date fecha = toDate("20/04/2022");
			Time hora = Time.valueOf("8:30:00");
			int nroPlazas = 50;

			servicio.comprarBillete(hora, fecha, ORIGEN, DESTINO, nroPlazas);

			LOGGER.info("NO se da cuenta de que no hay plazas MAL");
		} catch (SQLException e) {
			if (e.getErrorCode() == CompraBilleteTrenException.NO_PLAZAS) {
				LOGGER.info("Se da cuenta de que no hay plazas OK");
			}
		}

		// Prueba caso si existe y si hay plazas
		try {
			java.util.Date fecha = toDate("20/04/2022");
			Time hora = Time.valueOf("8:30:00");
			int nroPlazas = 5;

			servicio.comprarBillete(hora, fecha, ORIGEN, DESTINO, nroPlazas);

			con = pool.getConnection();
			st = con.prepareStatement(
					" SELECT IDVIAJE||IDTREN||IDRECORRIDO||FECHA||NPLAZASLIBRES||REALIZADO||IDCONDUCTOR||IDTICKET||CANTIDAD||PRECIO "
							+ " FROM VIAJES natural join tickets "
							+ " where idticket=3 and trunc(fechacompra) = trunc(current_date) ");
			rs = st.executeQuery();

			String resultadoReal = "";
			while (rs.next()) {
				resultadoReal += rs.getString(1);
			}

			String resultadoEsperado = "11120/04/2225113550";
			// LOGGER.info("R"+resultadoReal);
			// LOGGER.info("E"+resultadoEsperado);
			if (resultadoReal.equals(resultadoEsperado)) {
				LOGGER.info("Compra ticket OK");
			} else {
				LOGGER.info("Compra ticket MAL");
			}

		} catch (SQLException e) {
			LOGGER.info("Error inesperado MAL");
		}
	}
	
	public void ejecutarTestsAnularBilletes() {
	    Servicio servicio = new ServicioImpl();
	    PoolDeConexiones pool = PoolDeConexiones.getInstance();
	    Connection con = null;
	    PreparedStatement st = null;
	    ResultSet rs = null;

	    // Prueba caso no existe el ticket
	    try {
	        java.util.Date fecha = toDate("15/04/2010");
	        Time hora = Time.valueOf("12:00:00");
	        int nroPlazas = 1;
	        int ticket = 150;

	        servicio.anularBillete(hora, fecha, ORIGEN, DESTINO, nroPlazas, ticket);

	        LOGGER.info("NO se da cuenta de que no existe el ticket MAL");
	    } catch (SQLException e) {
	        if (e.getErrorCode() == CompraBilleteTrenException.NO_EXISTE_TICKET) {
	            LOGGER.info("Se da cuenta de que no existe el ticket OK");
	        }
	    }

	    // Prueba caso si existe pero no hay plazas
	    try {
	        java.util.Date fecha = toDate("20/04/2022");
	        Time hora = Time.valueOf("8:30:00");
	        int nroPlazas = 150;
	        int ticket = 1;

	        servicio.anularBillete(hora, fecha, ORIGEN, DESTINO, nroPlazas, ticket);

	        LOGGER.info("NO se da cuenta de que no hay plazas MAL");
	    } catch (SQLException e) {
	        if (e.getErrorCode() == CompraBilleteTrenException.MAS_PLAZAS) {
	            LOGGER.info("Se da cuenta de que no hay plazas OK");
	        }
	    }

	    // Prueba caso si existe y si hay plazas
	    try {
	        java.util.Date fecha = toDate("20/04/2022");
	        Time hora = Time.valueOf("8:30:00");
	        int nroPlazas = 1;
	        int ticket = 1;

	        servicio.anularBillete(hora, fecha, ORIGEN, DESTINO, nroPlazas, ticket);
	        
	        // Verificar si la fila correspondiente al billete anulado ya no existe
	        con = pool.getConnection();
	        st = con.prepareStatement(
	                "SELECT COUNT(*) FROM TICKETS WHERE idticket = ?");
	        st.setInt(1, ticket);
	        rs = st.executeQuery();
	        
	        int count = 0;
	        if (rs.next()) {
	            count = rs.getInt(1);
	        }

	        if (count == 0) {
	            LOGGER.info("La fila del billete ha sido borrada correctamente OK");
	        } else {
	            LOGGER.info("La fila del billete todavía existe en la base de datos MAL");
	        }
	    } catch (SQLException e) {
	        LOGGER.info("Error inesperado MAL");
	    } finally {
	        try {
	            if (rs != null) rs.close();
	            if (st != null) st.close();
	            if (con != null) con.close();
	        } catch (SQLException e) {
	            LOGGER.error("Error al cerrar recursos", e);
	        }
	    }
	}


	private java.util.Date toDate(String miString) { // convierte una cadena en fecha
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy"); // Las M en mayusculas porque sino interpreta
																		// minutos!!
			java.util.Date fecha = sdf.parse(miString);
			return fecha;
		} catch (ParseException e) {
			LOGGER.error(e.getMessage());
			return null;
		}
	}
}
