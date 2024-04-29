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

/**
 * ServicioImpl: Implementa los métodos para comprar y anular billetes.
 * 
 * @author <a href="mailto:cnd1003@alu.ubu.es">Christian Núñez</a>
 * @author <a href="mailto:dhp1005@alu.ubu.es">Daniel Hermelo</a>
 * @author <a href="mailto:izg1002@alu.ubu.es">Ignacio Zaldo</a>
 * @version 1.0
 * @since 1.0
 */
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

		try {
			con = PoolDeConexiones.getInstance().getConnection();
			// Primero, verificamos si el ticket existe y está vinculado al viaje adecuado
			String query = "SELECT idViaje, cantidad FROM tickets WHERE idTicket = ?";
			st = con.prepareStatement(query);
			st.setInt(1, ticket);
			rs = st.executeQuery();
			if (!rs.next()) {
				con.rollback();
				throw new CompraBilleteTrenException(3);
			}
			int idViaje = rs.getInt("idViaje");
			int cantidad = rs.getInt("cantidad");
			if (cantidad < nroPlazas) {
				con.rollback();
				throw new CompraBilleteTrenException(4);
			}

			// Actualizar la cantidad de plazas libres en el viaje correspondiente
			String updateViajeQuery = "UPDATE viajes SET nPlazasLibres = nPlazasLibres + ? WHERE idViaje = ?";
			PreparedStatement updateViajeSt = con.prepareStatement(updateViajeQuery);
			updateViajeSt.setInt(1, nroPlazas);
			updateViajeSt.setInt(2, idViaje);
			updateViajeSt.executeUpdate();

			// Actualizar el ticket si es necesario
			if (cantidad == nroPlazas) {
				// Si las plazas a anular son todas las compradas, podemos eliminar el ticket
				String deleteTicketQuery = "DELETE FROM tickets WHERE idTicket = ?";
				PreparedStatement deleteTicketSt = con.prepareStatement(deleteTicketQuery);
				deleteTicketSt.setInt(1, ticket);
				deleteTicketSt.executeUpdate();
			} else {
				// Reducir la cantidad de plazas en el ticket
				String updateTicketQuery = "UPDATE viajes SET nPlazasLibres = nPlazasLibres + ? WHERE idViaje = ?";
				PreparedStatement updateTicketSt = con.prepareStatement(updateTicketQuery);
				updateTicketSt.setInt(1, nroPlazas);
				updateTicketSt.setInt(2, idViaje);
				updateTicketSt.executeUpdate();
			}
			con.commit();
		} catch (SQLException e) {
			LOGGER.error("SQL Error: ", e);
			if (con != null)
				con.rollback();
			throw e;
		} finally {
			if (rs != null)
				rs.close();
			if (st != null)
				st.close();
			if (con != null)
				con.close();
		}
	}

	@Override
	public void comprarBillete(Time hora, Date fecha, String origen, String destino, int nroPlazas) throws SQLException {
	    Connection con = null;
	    PreparedStatement st = null;
	    ResultSet rs = null;
	    
	    // Convertimos las fechas y horas a formatos adecuados para la base de datos
	    java.sql.Date fechaSqlDate = new java.sql.Date(fecha.getTime());
		java.sql.Timestamp horaTimestamp = new java.sql.Timestamp(hora.getTime());
		
	    try {
	        con = PoolDeConexiones.getInstance().getConnection();
	        
	        // Consultamos si hay disponibilidad de plazas para el viaje elegido
	        String query = "SELECT idViaje, nPlazasLibres, precio FROM viajes JOIN recorridos ON viajes.idRecorrido = recorridos.idRecorrido " +
	            "WHERE recorridos.estacionOrigen = ? AND recorridos.estacionDestino = ? AND " +
	            "viajes.fecha = ? AND TO_CHAR(recorridos.horaSalida, 'HH24:MI') = TO_CHAR(?, 'HH24:MI')";
	        st = con.prepareStatement(query);
	        st.setString(1, origen);
	        st.setString(2, destino);
	        st.setDate(3, new java.sql.Date(fecha.getTime()));
	        st.setTime(4, hora);
	        rs = st.executeQuery();
	        
	        // Verificamos que el viaje exista y que tenga suficientes plazas disponibles
	        if (!rs.next()) {
	        	con.rollback();
	            throw new CompraBilleteTrenException(2); // En caso de no tener disponibilidad
	        }
	        int plazasDisponibles = rs.getInt("nPlazasLibres");
	        int idViaje = rs.getInt("idViaje");
	        float precio = rs.getFloat("precio");
	        if (plazasDisponibles < nroPlazas) {
	        	con.rollback();
	            throw new CompraBilleteTrenException(1); // En caso de no tener suficientes plazas
	        }
	        
	        // Insertamos un nuevo ticket dentro de la base de datos
	        PreparedStatement stInsert = con.prepareStatement("INSERT INTO tickets (idTicket, idViaje, fechaCompra, cantidad, precio)"
					+ " VALUES (seq_tickets.NEXTVAL, ?, CURRENT_DATE, ?, ?)");
				stInsert.setInt(1, idViaje);
				stInsert.setInt(2, nroPlazas);
				stInsert.setFloat(3, precio * nroPlazas);
				int filas = stInsert.executeUpdate();			
				if (filas == 1) con.commit();
				
				// Actualizamos la cantidad de plazas disponibles para el viaje en concreto
				String updateQuery = "UPDATE viajes SET nPlazasLibres = nPlazasLibres - ? WHERE idViaje = ?";
				PreparedStatement updateSt = con.prepareStatement(updateQuery);
				updateSt.setInt(1, nroPlazas);
				updateSt.setInt(2, idViaje);
				filas = updateSt.executeUpdate();
				if (filas == 1) con.commit();
	        
	        con.commit();
	    } catch (SQLException e) {
	        LOGGER.error("SQL Error: ", e); // La línea esta la podríamos comentar para que no salga un texto largo al ejecutar los test
	        if (con != null) con.rollback(); // Si hay algún error, hacemos un rollback para deshacer los cambios
	        throw e;  // Lanzamos la excepción
	    } finally {
	    	// Cerramos el resultset, la conexión y preparedstatements
	        if (rs != null) rs.close();
	        if (st != null) st.close();
	        if (con != null) con.close();
	    }
	}
}
