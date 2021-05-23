package series;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SeriesDatabase {
	private static Connection conn_ = null;
	
	public SeriesDatabase() {
		
	}

	public boolean openConnection() {
		if(conn_ == null) {
			try {
				String drv = "com.mysql.cj.jdbc.Driver";
				Class.forName(drv);
				System.out.println("Driver cargado correctamente");
			} catch (ClassNotFoundException _e){
				System.out.println("Error, el driver no se ha encontrado");
				_e.printStackTrace();
				return false;
			}
			
			try {
				String serverAddress = "localhost:3306";
				String db = "series";
				String user = "series_user";
		        String pass = "series_pass";
				String url = "jdbc:mysql://" + serverAddress + "/" + db;
				conn_ = DriverManager.getConnection(url, user, pass);					
				System.out.println("Base de Datos cargada correctamente");
				return true;
			}catch (SQLException _e) {
				conn_ = null;
				System.out.println("Error, la base de datos no se ha encontrado");
				_e.printStackTrace();
				return false;
			}
		}else {
			System.out.println("Base de Datos cargada anteriormente");
			return false;
		}
	}

	public boolean closeConnection() {
		if(conn_ != null) {
			try {
				conn_.close();	
				System.out.println("Base de Datos cerrada correctamente");
				return true;
			} catch(SQLException _e) {
				System.out.println("Error, la base de datos no se ha encontrado");
				_e.printStackTrace();
				return false;
			}			
		}else {
			return true;
		}
	}

	public boolean createTableCapitulo() {
		openConnection();
		System.out.println("Hello World");
		return false;
	}

	public boolean createTableValora() {
		openConnection();
		return false;
	}

	public int loadCapitulos(String fileName) {
		openConnection();
		return 0;
	}

	public int loadValoraciones(String fileName) {
		openConnection();
		return 0;
	}

	public String catalogo() {
		openConnection();
		return null;
	}
	
	public String noHanComentado() {
		openConnection();
		return null;
	}

	public double mediaGenero(String genero) {
		openConnection();
		return 0.0;
	}
	
	public double duracionMedia(String idioma) {
		openConnection();
		return 0.0;
	}

	public boolean setFoto(String filename) {
		openConnection();
		return false;
	}

}
