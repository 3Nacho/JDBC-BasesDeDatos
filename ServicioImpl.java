package lsi.ubu.servicios;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.excepciones.CompraBilleteTrenException;
import lsi.ubu.util.PoolDeConexiones;

public class ServicioImpl implements Servicio {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicioImpl.class);

	@Override
	public void anularBillete(Time hora, java.util.Date fecha, String origen, String destino, int nroPlazas, int ticket)
			throws SQLException {
		PoolDeConexiones pool = PoolDeConexiones.getInstance();

		/* Conversiones de fechas y horas */
		java.sql.Date fechaSqlDate = new java.sql.Date(fecha.getTime());
		java.sql.Timestamp horaTimestamp = new java.sql.Timestamp(hora.getTime());

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;

		// A completar por el alumno
	}

	@Override
	public void comprarBillete(Time hora, Date fecha, String origen, String destino, int nroPlazas) throws SQLException {
	    Connection con = null;
	    PreparedStatement st = null;
	    ResultSet rs = null;
	    
	    java.sql.Date fechaSqlDate = new java.sql.Date(fecha.getTime());
		java.sql.Timestamp horaTimestamp = new java.sql.Timestamp(hora.getTime());
		
	    try {
	        con = PoolDeConexiones.getInstance().getConnection();
	        String query = "SELECT idViaje, nPlazasLibres, precio FROM viajes JOIN recorridos ON viajes.idRecorrido = recorridos.idRecorrido " +
	            "WHERE recorridos.estacionOrigen = ? AND recorridos.estacionDestino = ? AND " +
	            "viajes.fecha = ? AND TO_CHAR(recorridos.horaSalida, 'HH24:MI') = TO_CHAR(?, 'HH24:MI')";
	        st = con.prepareStatement(query);
	        st.setString(1, origen);
	        st.setString(2, destino);
	        st.setDate(3, new java.sql.Date(fecha.getTime()));
	        st.setTime(4, hora);
	        rs = st.executeQuery();
	        if (!rs.next()) {
	        	con.rollback();
	            throw new CompraBilleteTrenException(2);
	        }
	        int plazasDisponibles = rs.getInt("nPlazasLibres");
	        int idViaje = rs.getInt("idViaje");
	        float precio = rs.getFloat("precio");
	        if (plazasDisponibles < nroPlazas) {
	        	con.rollback();
	            throw new CompraBilleteTrenException(1);
	        }
	        PreparedStatement stInsert = con.prepareStatement("INSERT INTO tickets (idTicket, idViaje, fechaCompra, cantidad, precio)"
					+ " VALUES (seq_tickets.NEXTVAL, ?, CURRENT_DATE, ?, ?)");
				stInsert.setInt(1, idViaje);
				stInsert.setInt(2, nroPlazas);
				stInsert.setFloat(3, precio * nroPlazas);
				int filas = stInsert.executeUpdate();			
				if (filas == 1) con.commit();
				
				// Actualizar la cantidad de plazas disponibles
				String updateQuery = "UPDATE viajes SET nPlazasLibres = nPlazasLibres - ? WHERE idViaje = ?";
				PreparedStatement updateSt = con.prepareStatement(updateQuery);
				updateSt.setInt(1, nroPlazas);
				updateSt.setInt(2, idViaje);
				filas = updateSt.executeUpdate();
				if (filas == 1) con.commit();
	        
	        con.commit();
	    } catch (SQLException e) {
	        LOGGER.error("SQL Error: ", e); // La línea esta la podríamos comentar para que no salga texto de más al ejecutar los test
	        if (con != null) con.rollback();
	        throw e;
	    } finally {
	        if (rs != null) rs.close();
	        if (st != null) st.close();
	        if (con != null) con.close();
	    }
	}
}
