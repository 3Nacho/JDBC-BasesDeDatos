package lsi.ubu.excepciones;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CompraBilleteTrenException: Implementa las excepciones contextualizadas de la
 * transaccion de CompraBilleteTren
 * 
 * @author <a href="mailto:jmaudes@ubu.es">Jes�s Maudes</a>
 * @author <a href="mailto:rmartico@ubu.es">Ra�l Marticorena</a>
 * @version 1.0
 * @since 1.0
 */
public class CompraBilleteTrenException extends SQLException {

	private static final long serialVersionUID = 1L;

	private static final Logger LOGGER = LoggerFactory.getLogger(CompraBilleteTrenException.class);

	public static final int NO_PLAZAS = 1;
	public static final int NO_EXISTE_VIAJE = 2;
	public static final int NO_EXISTE_TICKET = 3;
	public static final int MAS_PLAZAS = 4;

	private int codigo; // = -1;
	private String mensaje;

	public CompraBilleteTrenException(int code) {
		this.codigo = code;
		if(code == 1) {
			mensaje = "No hay plazas suficientes";
		}else if (code == 2) {
			mensaje = "No existe viaje para tal fecha, hora, origen y destino.";
		}else if (code == 3) {
			mensaje = "No existe el ticket.";
		}else if (code == 4) {
			mensaje = "Se ha intentado eliminar mas plazas de las que hay en el ticket.";
		}

		LOGGER.debug(mensaje);

		// Traza_de_pila
		for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
			LOGGER.debug(ste.toString());
		}
	}

	@Override
	public String getMessage() { // Redefinicion del metodo de la clase Exception
		return mensaje;
	}

	@Override
	public int getErrorCode() { // Redefinicion del metodo de la clase SQLException
		return codigo;
	}
}
