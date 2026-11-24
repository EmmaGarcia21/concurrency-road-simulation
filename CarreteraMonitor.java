package cc.carretera;

import es.upm.aedlib.Pair;
import es.upm.babel.cclib.Monitor;
import java.util.*;

public class CarreteraMonitor implements Carretera {
	boolean[][] carretera; // matriz que representa si los carriles y segmentos están ocupados
	Map<String, Pair<Pos, Integer>> estado;
	Monitor mutex;
	List<Pair<String, Monitor.Cond>> peticionesEntrar;
	List<Pair<String, Monitor.Cond>> peticionesAvanzar;
	List<Pair<String, Monitor.Cond>> peticionesCirculando;
	int segmentos;
	int carriles;

	public CarreteraMonitor(int segmentos, int carriles) {
		this.segmentos = segmentos;
		this.carriles = carriles;
		this.carretera = new boolean[segmentos][carriles];
		this.estado = new HashMap<>();
		this.mutex = new Monitor();
		this.peticionesEntrar = new ArrayList<>();
		this.peticionesAvanzar = new ArrayList<>();
		this.peticionesCirculando = new ArrayList<>();
	}

	private int carrilLibre(int segmento) {
		for (int i = 0; i < carriles; i++)
			if (!carretera[segmento - 1][i]) 
				return i;
		return -1; // no hay carril libre
	}

	private void desbloquearEntrar() {
		Iterator<Pair<String, Monitor.Cond>> it = peticionesEntrar.iterator();
		if (it.hasNext()) {
			Pair<String, Monitor.Cond> par = it.next();
			if (carrilLibre(1) != -1) {
				par.getRight().signal(); // despertar coche bloqueado para entrar
				it.remove(); // elimina petición
			}
		}
	}

	private void desbloquearAvanzar() {
		Iterator<Pair<String, Monitor.Cond>> it = peticionesAvanzar.iterator();
		if (it.hasNext()) {
			Pair<String, Monitor.Cond> par = it.next();
			String id = par.getLeft();
			if (estado.containsKey(id)) {
				Pos pos = estado.get(id).getLeft();
				if (pos.getSegmento() < segmentos && carrilLibre(pos.getSegmento() + 1) != -1) {
					par.getRight().signal(); // despertar el coche que puede avanzar
					it.remove();
				}
			}
		}
	}

	private void desbloquearCirculando() {
		Pair<String, Monitor.Cond> parDesbloquear = null;
		Iterator<Pair<String, Monitor.Cond>> it = peticionesCirculando.iterator();
		
		while (it.hasNext() && parDesbloquear == null) {
			Pair<String, Monitor.Cond> par = it.next();
			String id = par.getLeft();
			if (estado.containsKey(id) && estado.get(id).getRight() == 0) { // ticks agotados
				parDesbloquear = par; // asignar par que cumple las condiciones
				it.remove();
			}
		}
		
		if (parDesbloquear != null)
			parDesbloquear.getRight().signal(); // despertar coche circulando con 0 ticks
	}

	public Pos entrar(String id, int tks) {
		mutex.enter();
		try {
			Monitor.Cond cond = mutex.newCond();
			if (carrilLibre(1) == -1) {
				peticionesEntrar.add(new Pair<>(id, cond));
				cond.await();
				peticionesEntrar.removeIf(p -> p.getLeft().equals(id));
			}
			int carril = carrilLibre(1); // obtener carril libre en segmento 1
			carretera[0][carril] = true; // marcar carril ocupado
			Pos pos = new Pos(1, carril + 1);
			estado.put(id, new Pair<>(pos, tks)); // guarda la posicion y ticks
			return pos;
		} finally {
			mutex.leave();
		}
	}

	public Pos avanzar(String id, int tks) {
		mutex.enter();
		try {
			Pair<Pos, Integer> estadoCoche = estado.get(id);
			Pos posActual = estadoCoche.getLeft();
			int segmento = posActual.getSegmento();

			if (segmento == segmentos) // comprobar que está en el último segmento
				return posActual;

			int carrilNuevo = carrilLibre(segmento + 1);
			if (carrilNuevo == -1) {
				Monitor.Cond cond = mutex.newCond();
				peticionesAvanzar.add(new Pair<>(id, cond));
				cond.await();
				peticionesAvanzar.removeIf(p -> p.getLeft().equals(id));
				carrilNuevo = carrilLibre(segmento + 1);
				if (carrilNuevo == -1)
					return posActual; // no hay carril libre, sigue en la misma posición
			}
			carretera[segmento - 1][posActual.getCarril() - 1] = false; // libera carril anterior
			carretera[segmento][carrilNuevo] = true; // ocupa nuevo carril

			Pos nuevaPos = new Pos(segmento + 1, carrilNuevo + 1);
			estado.put(id, new Pair<>(nuevaPos, tks)); // actualiza posicion y ticks

			desbloquearAvanzar(); // intenta despertar otros coches para avanzar
			desbloquearEntrar(); // intenta despertar otros coches para entrar

			return nuevaPos;

		} finally {
			mutex.leave();
		}
	}

	public void circulando(String id) {
		mutex.enter();
		Pair<Pos, Integer> par = estado.get(id);
		int tks = par.getRight();
		if(tks > 0) {
			Monitor.Cond cond = mutex.newCond();
			peticionesCirculando.add(new Pair<>(id, cond));
			cond.await();
		}
		desbloquearCirculando(); // intenta despertar otros coches circulando
		mutex.leave();
	}

	public void salir(String id) {
		mutex.enter();
		try {
			if (!estado.containsKey(id)) return;
			Pos pos = estado.get(id).getLeft();
			carretera[pos.getSegmento() - 1][pos.getCarril() - 1] = false; // liberar carril
			estado.remove(id); // eliminar coche del mapa estado

			desbloquearEntrar(); // puede haber sitio para nuevos coches
			desbloquearAvanzar(); // puede haber sitio para entrar
		} finally {
			mutex.leave();
		}
	}

	public void tick() {
		mutex.enter();
		
		// decrementar ticks para todos los coches
		for(String id: estado.keySet()) { 
			Pair<Pos, Integer> par = estado.get(id);
			estado.put(id, new Pair<Pos, Integer>(par.getLeft(), Math.max(par.getRight()-1, 0)));
		}

		desbloquearCirculando(); // despierta coches con ticks a 0

		mutex.leave();
	}
}