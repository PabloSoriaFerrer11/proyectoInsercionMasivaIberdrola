package com.pasofe.insercionMasivaIbertrola;

import java.util.Scanner;

import org.bson.types.ObjectId;

//InsercionMasiva
public class App {
    static conexion cn = new conexion();
	static Scanner sc = new Scanner(System.in);
    
	public static void main( String[] args ){


	int op=0;
    	
    	do {
    		System.out.println("");
    		System.out.println("Introduce");
    		System.out.println("::::::::::::::::::::::::::::::::::::::::::::::::");
    		System.out.println("1.- Insercion Masiva (INSERT ONE) en CLIENTES");
    		System.out.println("2.- Insercion Masiva (INSERT MANY) en CLIENTES");
    		System.out.println("3.- Eliminar todos los clientes");
    		System.out.println("4.- Insertar Factura");
    		System.out.println("5.- Eliminar Factura");
    		System.out.println("6.- Crear factura de un consumo");
    		System.out.println("7.- Borrar factura y consumo");
    		System.out.println("8.- SALIR");
    		System.out.println(":::::::::::::::::::::::::::::::::::::::::::::::::");
    		op = sc.nextInt();
    		sc.nextLine();
    		
    		switch(op) {
    		case 1: cn.insertarClientesOne(); op = 8; break;
    		case 2: cn.insertarClientesMany();op = 8;  break;
    		case 3: cn.eliminarClientes(); break;
    		case 4: cn.insertarConsumo(); break;
    		case 5: cn.eliminarConsumos(); break;
    		case 6: crearFacturaEspecifica(); break;
    		case 7: borrarFacturaCascada(); break;
    		default: break;
    		}
    	}while(op!=8);
       
       
      
    }

	private static void crearFacturaEspecifica() {
		
		System.out.println("Introduce el id del consumo: ");
		String idc = sc.nextLine();
		
		ObjectId objId = new ObjectId(idc);
		
		System.out.println("Introduce la coleccion del consumo: ");
		String col = sc.nextLine();
		col = col.toLowerCase();
		
		cn.insertarFacturas(objId, col);
		
	}

	private static void borrarFacturaCascada() {
		System.out.println("Introduce el nombre del ciente que tiene la factura: (ClienteX)");
		String c = sc.nextLine();
		
		System.out.println("Introduce el mes que quieres borrar: ");
		String m = sc.nextLine();
		m=m.toLowerCase();
		
		cn.eliminarFactura(c, m);
		
	}
}
