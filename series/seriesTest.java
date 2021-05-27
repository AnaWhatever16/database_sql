package series;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class seriesTest {
	static SeriesDatabase serieData;
	@BeforeAll
	static void initClass() {
		serieData = new SeriesDatabase();
	}

	@BeforeEach
	void open() {
		assertTrue(serieData.openConnection());
		assertFalse(serieData.openConnection());
		assertTrue(serieData.closeConnection());
		assertTrue(serieData.openConnection());
	}
	
	@AfterEach
	void close() {
		assertTrue(serieData.closeConnection());
	}
	
	@Test
	void createCapitulo() {
		assertTrue(serieData.createTableCapitulo());
		assertFalse(serieData.createTableCapitulo());
	}
	
	@Test
	void createValora() {
		assertTrue(serieData.createTableValora());
		assertFalse(serieData.createTableValora());
	}
	
	@Test
	void insertCapitulos() {
		assertEquals(400, serieData.loadCapitulos("capitulos.csv"));
	}
	
	@Test
	void insertValoraciones() {
		assertEquals(351, serieData.loadValoraciones("valoraciones.csv"));
	}

}
