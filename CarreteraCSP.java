package cc.carretera;

import org.jcsp.lang.*;
import java.util.*;

public class CarreteraCSP implements Carretera, CSProcess {
  // TODO: Declaración de canales
  // Ej. private Any2One chOp;

    private final Any2OneChannel chEntrar;
    private final Any2OneChannel chAvanzar;
    private final Any2OneChannel chCirculando;
    private final Any2OneChannel chSalir;
    private final Any2OneChannel chTick;
  
  // Configuración de la carretera
  private final int segmentos;
  private final int carriles;
	
	//clase para saber la estructura del mensaje que envían los clientes al servidor
    private static class Mensaje {
        String id;
        int tks;
        ChannelOutput resp;
        
        Mensaje(String id, int tks, ChannelOutput resp) {
            this.id = id;
            this.tks = tks;
            this.resp = resp;
        }
    }

  public CarreteraCSP(int segmentos, int carriles) {
    this.segmentos = segmentos;
    this.carriles = carriles;

    // TODO: Creación de canales para comunicación con el servidor
    // Ej. chOp = Channel.any2one();
	this.chEntrar = Channel.any2one();
	this.chAvanzar = Channel.any2one();
	this.chCirculando = Channel.any2one();
	this.chSalir = Channel.any2one();
	this.chTick = Channel.any2one();

    // Puesta en marcha del servidor: alternativa sucia (desde el
    // punto de vista de CSP) a Parallel que nos ofrece JCSP para
    // poner en marcha un CSProcess
    new ProcessManager(this).start();
  }

  public Pos entrar(String car, int tks) {
    // TODO: código que ejecuta el cliente para enviar/recibir un
    // mensaje al server para que ejecute entrar
	One2OneChannel resp = Channel.one2one();
	chEntrar.out().write(new Mensaje(car, tks, resp.out()));
	return (Pos)resp.in().read();
  }

  public Pos avanzar(String car, int tks) {
    // TODO: código que ejecuta el cliente para enviar/recibir un
    // mensaje al server para que ejecute avanzar
	One2OneChannel resp = Channel.one2one();
    chAvanzar.out().write(new Mensaje(car, tks, resp.out()));
    return (Pos)resp.in().read();
  }

  public void salir(String car) {
    // TODO: código que ejecuta el cliente para enviar un mensaje al
    // server para que ejecute salir
	One2OneChannel resp = Channel.one2one();
	chSalir.out().write(new Mensaje(car, 0, resp.out()));
	resp.in().read();
  }

  public void circulando(String car) {
    // TODO: código que ejecuta el cliente para enviar un mensaje al
    // server para que ejecute circulando
	One2OneChannel resp = Channel.one2one();
	chCirculando.out().write(new Mensaje(car, 0, resp.out()));
	resp.in().read();
  }

  public void tick() {
    // TODO: código que ejecuta el cliente para enviar un mensaje al
    // server para que ejecute tick
	One2OneChannel resp = Channel.one2one();
	chTick.out().write(new Mensaje(null, 0, resp.out()));
	resp.in().read();
  }

  // Código del servidor
  public void run() {
    // TODO: declaración e inicialización del estado del recurso
	Map<String, Pos> posiciones = new HashMap<>();
	Map<String, Integer> ticksRestantes = new HashMap<>();
		
    // TODO: declaración e inicialización de estructuras de datos para
    // almacenar peticiones de los clientes
	Queue<Mensaje> colaEntrar = new LinkedList<>();
	Queue<Mensaje> colaAvanzar = new LinkedList<>();
	Queue<Mensaje> colaCirculando = new LinkedList<>();
	
    // TODO: declaración e inicialización de arrays necesarios para
    // poder hacer la recepción no determinista (Alternative)
	Guard[] guards = {
		chEntrar.in(),
		chAvanzar.in(),
		chCirculando.in(),
		chSalir.in(),
		chTick.in()
	};
	Alternative servicios = new Alternative(guards);

    // TODO: cambiar null por el array de canales


    // Bucle principal del servicio
    while(true){
      // TODO: declaración de variables auxiliares
      int servicio;

      // TODO: cálculo de las guardas

      // TODO: cambiar null por el array de guardas
      servicio = servicios.fairSelect();

      // TODO: ejecutar la operación solicitada por el cliente
	  Mensaje mensaje;
      switch (servicio){
      case 0:
        // TODO: ejecutar operación 0 o almacenar la petición y
        // responder al cliente si es posible
		mensaje = (Mensaje) chEntrar.in().read();
        colaEntrar.add(mensaje);
        break;
		
	  case 1:
        mensaje = (Mensaje) chAvanzar.in().read();
        colaAvanzar.add(mensaje);
        break;
      
	  case 2:
        mensaje = (Mensaje) chCirculando.in().read();
        colaCirculando.add(mensaje);
      break;

      case 3:
        mensaje = (Mensaje) chSalir.in().read();
        posiciones.remove(mensaje.id);
        ticksRestantes.remove(mensaje.id);
        mensaje.resp.write(null);
       break;

       case 4:
        mensaje = (Mensaje) chTick.in().read();
        for (String id : ticksRestantes.keySet()) {
        int t = ticksRestantes.get(id);
        if (t > 0)
            ticksRestantes.put(id, t - 1);
        }
        mensaje.resp.write(null);
        break;
      }

      // Ahora intentamos atender todas las peticiones que estaban en espera
      boolean cambio;
      do {
        cambio = false;

        Queue<Mensaje> tmpEntrar = new LinkedList<>(colaEntrar);
        colaEntrar.clear();
        for (Mensaje m : tmpEntrar) {
          boolean colocado = false;
          if (!posiciones.containsKey(m.id)) {
            for (int c = 1; c <= carriles; c++) {
              Pos pos = new Pos(1, c);
              boolean libre = true;
              for (Pos p : posiciones.values()) {
                if (p.equals(pos)) {
                  libre = false;
                  break;
                }
              }
              if (libre) {
                posiciones.put(m.id, pos);
                ticksRestantes.put(m.id, m.tks);
                m.resp.write(pos); // notificamos al coche su posición
                colocado = true;
                cambio = true; // se produce cambio 
                break;
              }
            }
          }
          if (!colocado) colaEntrar.add(m);// aún no puede entrar
        }
	    
		//Procesamos peticiones de avanzar
        Queue<Mensaje> tmpAvanzar = new LinkedList<>(colaAvanzar);
        colaAvanzar.clear();
        for (Mensaje m : tmpAvanzar) {
          Pos actual = posiciones.get(m.id);
          if (actual != null && actual.getSegmento() < segmentos && ticksRestantes.get(m.id) == 0) {
            boolean avanzado = false;
            for (int c = 1; c <= carriles; c++) {
              Pos nueva = new Pos(actual.getSegmento() + 1, c);
              boolean libre = true;
              for (Pos ocupada : posiciones.values()) {
                if (ocupada.equals(nueva)) {
                  libre = false;
                  break;
                }
              }
              if (libre) {
                posiciones.put(m.id, nueva);
                ticksRestantes.put(m.id, m.tks);
                m.resp.write(nueva);
                avanzado = true;
                cambio = true;
                break;
              }
            }
            if (!avanzado) colaAvanzar.add(m);
          } else {
            colaAvanzar.add(m);
          }
        }

        Queue<Mensaje> tmpCirc = new LinkedList<>(colaCirculando);
        colaCirculando.clear();
        for (Mensaje m : tmpCirc) {
          if (ticksRestantes.containsKey(m.id) && ticksRestantes.get(m.id) == 0) {
            m.resp.write(null);
            cambio = true;
          } else {
            colaCirculando.add(m);
          }
        }

      } while (cambio);// seguimos intentando mientras haya desbloqueos
    }
  }
}