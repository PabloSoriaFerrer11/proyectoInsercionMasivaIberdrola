package com.pasofe.insercionMasivaIbertrola;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

//InsercionMasiva
/*
 * MongoClientURI connectionString = new MongoClientURI("mongodb://localhost:27017");
MongoClient mongoClient = new MongoClient(connectionString);
CodecRegistry pojoCodecRegistry = org.bson.codecs.configuration.CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), org.bson.codecs.configuration.CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));
MongoDatabase database = mongoClient.getDatabase("testdb").withCodecRegistry(pojoCodecRegistry);  
 */

public class conexion {
	
	public int cantidad = 10;
	public int cantidadMax = 10000;
	//ESTE MES DEBERÄ IR CAMBIANDO CON EL TIEMPO
	public String mes = "febrero";
	

	public MongoClient conexion() {
		MongoClient mongo = null;
        mongo = new MongoClient("localhost", 27017);
        return mongo;
	}
	
	public void insertarClientesOne() {
		MongoClient cn = conexion();
	
		MongoDatabase db = cn.getDatabase("ibertrola");
	
		
		MongoCollection col = db.getCollection("clientes");
		
		
		//List<Document> list = new ArrayList<Document>();
		
		String[] provincia = {"valencia", "madrid", "cuenca", "soria","barcelona", "sevilla"};
		int[] arraytlf = {666666666};
		

		
		//Pruebas realizadas con 1000
		for(int i=0; i<cantidad;i++) {
			
			//VALORES ALEATORIOS
			int aux = (int)(Math.random()*(provincia.length));
			
			int num = (int)(Math.random()*110+1);
			
			String provinciaCiudad = provincia[aux];
			
			String idTabla = mes + provinciaCiudad;
			
			
			System.out.println("");
			System.out.println("___________________________");
			System.out.println("| " + num + " | " + aux + " | " + provinciaCiudad + " | " + idTabla + " | ");
			System.out.println("::::::::::::::::::::::::::");
			

			

			
			
			
			//.................................................................................................................................................
			//Creando el documento principal llamado cliente
			Document CLIENTE = new Document();
			


			CLIENTE.append("nombre","Cliente"+i);
			CLIENTE.append("dni", String.valueOf((int)(Math.random()*1000000+1)+"A")) ;
			
			//Creando Array de Telefonos
			ArrayList<String> tel = new ArrayList<String>();
			tel.add("666666666");
			
			CLIENTE.append("telefonos", tel);
			
			
			//Creadno el cocumento de Direccion Principals
			Document direccion = new Document();
			direccion.append("provincia", provinciaCiudad);
			direccion.append("ciudad", provinciaCiudad);
			direccion.append("Calle", "Calle Inventada");
			direccion.append("numero", num);
			
			CLIENTE.append("direccionPrincipal", direccion);
			
			//Creando Array de Facturas
			ArrayList<Document> fac = new ArrayList<Document>();
			
			
			
			Document factura = new Document();
			factura.append("año", 2021);
			factura.append("mes-provincia", idTabla);
			fac.add(factura);
			
			CLIENTE.append("facturas", fac);
			
			col.insertOne(CLIENTE);
		}
		


		System.out.println("Done");
	}
	
	
	public void eliminarClientes() {
		MongoClient cn = conexion();
		
		MongoDatabase db = cn.getDatabase("ibertrola");
	
		MongoCollection col = db.getCollection("clientes");
		
		long count;
		try {
			count = -1;
			do {
				count = col.countDocuments();
				
				System.out.println(count);
				
				Document aux = (Document) col.find().first();
				 
				col.deleteOne(aux);
	
				 
			}while(count!=1 || count==0);
		}catch(IllegalArgumentException e) {
			System.out.println("");
		}
		
		System.out.println("Done Deleting Clientes");
		
	}

	public void eliminarConsumos() {
		MongoClient cn = conexion();
		
		MongoDatabase db = cn.getDatabase("ibertrola");
	
		MongoCollection col = db.getCollection("febrerobarcelona");
		long count;
		try {
			count = -1;
			do {
				count = col.countDocuments();
				
				System.out.println(count);
				
				Document aux = (Document) col.find().first();
				 
				col.deleteOne(aux);
	
				 
			}while(count!=1 || count==0);
		}catch(IllegalArgumentException e) {
			System.out.println("");
		}
		System.out.println("Done Deleting Barcelona");
		//-------------------------------------------------------------
		
		col = db.getCollection("febrerocuenca");
		try {
			count = -1;
			do {
				count = col.countDocuments();
				
				System.out.println(count);
				
				Document aux = (Document) col.find().first();
				 
				col.deleteOne(aux);
	
				 
			}while(count!=1 || count==0);
		}catch(IllegalArgumentException e) {
			System.out.println("");
		}
		System.out.println("Done Deleting Cuenca");
		//-------------------------------------------------------------		
		
		col = db.getCollection("febreromadrid");
		try {
			count = -1;
			do {
				count = col.countDocuments();
				
				System.out.println(count);
				
				Document aux = (Document) col.find().first();
				 
				col.deleteOne(aux);
	
				 
			}while(count!=1 || count==0);
		}catch(IllegalArgumentException e) {
			System.out.println("");
		}
		System.out.println("Done Deleting Madrid");
		//-------------------------------------------------------------
		col = db.getCollection("febrerosevilla");
		try {
			count = -1;
			do {
				count = col.countDocuments();
				
				System.out.println(count);
				
				Document aux = (Document) col.find().first();
				 
				col.deleteOne(aux);
	
				 
			}while(count!=1 || count==0);
		}catch(IllegalArgumentException e) {
			System.out.println("");
		}
		System.out.println("Done Deleting Sevilla");
		//-------------------------------------------------------------
		col = db.getCollection("febrerosoria");
		try {
			count = -1;
			do {
				count = col.countDocuments();
				
				System.out.println(count);
				
				Document aux = (Document) col.find().first();
				 
				col.deleteOne(aux);
	
				 
			}while(count!=1 || count==0);
		}catch(IllegalArgumentException e) {
			System.out.println("");
		}
		System.out.println("Done Deleting Soria");
		//-------------------------------------------------------------
		col = db.getCollection("febrerovalencia");
		try {
			count = -1;
			do {
				count = col.countDocuments();
				
				System.out.println(count);
				
				Document aux = (Document) col.find().first();
				 
				col.deleteOne(aux);
	
				 
			}while(count!=1 || count==0);
		}catch(IllegalArgumentException e) {
			System.out.println("");
		}
		System.out.println("Done Deleting Valencia");
		//-------------------------------------------------------------
	}
	
	public void insertarClientesMany() {
		MongoClient cn = conexion();
		
		MongoDatabase db = cn.getDatabase("ibertrola");
	
		
		MongoCollection col = db.getCollection("clientes");
		
		
		//List<Document> list = new ArrayList<Document>();
		
		String[] provincia = {"valencia", "madrid", "cuenca", "soria","barcelona", "sevilla"};
		int[] arraytlf = {666666666};
		 List<Document> documents = new ArrayList<Document>();

		
			//Pruebas realizadas con 1000
			for(int i=0; i<cantidad;i++) {
				
				//VALORES ALEATORIOS
				int aux = (int)(Math.random()*(provincia.length));
				
				int num = (int)(Math.random()*110+1);
				
				String provinciaCiudad = provincia[aux];
				
				String idTabla = mes + provinciaCiudad;
				
				
				System.out.println("");
				System.out.println("___________________________");
				System.out.println("| " + num + " | " + aux + " | " + provinciaCiudad + " | " + idTabla + " | ");
				System.out.println("::::::::::::::::::::::::::");
				

			
			//.................................................................................................................................................
			//Creando el documento principal llamado cliente
			Document CLIENTE = new Document();
			


			CLIENTE.append("nombre","Cliente"+i);
			CLIENTE.append("dni", String.valueOf((int)(Math.random()*1000000+1)+"A")) ;
			
			//Creando Array de Telefonos
			ArrayList<String> tel = new ArrayList<String>();
			tel.add("666666666");
			
			CLIENTE.append("telefonos", tel);
			
			
			//Creadno el cocumento de Direccion Principals
			Document direccion = new Document();
			direccion.append("provincia", provinciaCiudad);
			direccion.append("ciudad", provinciaCiudad);
			direccion.append("Calle", "Calle Inventada");
			direccion.append("numero", num);
			
			CLIENTE.append("direccionPrincipal", direccion);
			
			//Creando Array de Facturas
			ArrayList<Document> fac = new ArrayList<Document>();
			
			
			
			Document factura = new Document();
			factura.append("año", 2021);
			factura.append("mes-provincia", idTabla);
			fac.add(factura);
			
			CLIENTE.append("facturas", fac);
			
			
			
			documents.add(CLIENTE);
			
			
		}
	
		col.insertMany(documents);

		System.out.println("Done");
		
	}
	
	public void insertarConsumo() {
		//Se cogerá el primer cliente "cliente1" se cogerá su provincia, se creara o buscara la colección 
		//con su CIUDAD-MES en esta COLECCION (tabla) se insertará el consumo con un id del usuario.
		
		MongoClient cn = conexion();
		
		MongoDatabase db = cn.getDatabase("ibertrola");
	
		
		MongoCollection col = db.getCollection("clientes");
		
		////Bson bsonFilter = Filters.eq("name", "Troy");
		
		for(int i=0;i<cantidad;i++) {
			String c = "Cliente"+i;
			
			System.out.println(c);
			
			FindIterable<Document> docs = col.find(Filters.eq("nombre",c));
			
			Document first = docs.first();
			
			ObjectId id =  (ObjectId) first.get("_id");
		    ArrayList<Document> lista = (ArrayList<Document>) first.get("facturas");
		    		    
		    for(int j=0; j<lista.size();j++){
		    	
		    	Document docFactura = lista.get(j);
		    	
		    	String mp = (String) docFactura.getString("mes-provincia");
		
		    	
		    	//LLamar a metodoCrearConsumo
		    	System.out.println("Mes-provincia  " + mp);
		    	crearConsumo(id, mp);
		    	
		    }
		}
	}

	private void crearConsumo(ObjectId id, String mp) {
		Document consumo = new Document("ajenaCliente", id);
		
		ArrayList<Document> dia = new ArrayList<Document>();

		
		int whconsum = 0, whproduc = 0;
		
		for (int i=0; i<30; i++) {
			//para cada día se crea un documento
			Document diaAux = new Document();
			ArrayList<Document> Cadahoras = new ArrayList<Document>();
			
			for(int j=0;j<24;j++) {
				
				Document a = new Document();
				
				whconsum =  (int) (Math.random()*260+1);
				whproduc =  (int) (Math.random()*50+1);
				

				a.append("whconsumo", whconsum);
				a.append("whproducidos", whproduc);
				
				Cadahoras.add(a);
				
			}
			//el document ya con las horas se introduce

			diaAux.append("gastoHoras", Cadahoras);
			dia.add(diaAux);

		}
		consumo.append("dia", dia);
		
	
		MongoClient cn = conexion();
		
		MongoDatabase db = cn.getDatabase("ibertrola");
	
		
		MongoCollection col = db.getCollection(mp);
		
		col.insertOne(consumo);
	}

	public void insertarFacturas(ObjectId id, String colec) {
		//-----------------
	int totalconsumido = 0, totalproducido = 0;
		
		MongoClient cn = conexion();
		
		MongoDatabase db = cn.getDatabase("ibertrola");
	
		MongoCollection col = db.getCollection(colec);

		
		Bson query = Filters.eq("_id", id);
		
		Document docConsumo = (Document) col.find(query).first();
		
		ObjectId idCliente = docConsumo.getObjectId("_id");
		
		ArrayList<Document> x = (ArrayList<Document>) docConsumo.get("dia");
		
		for(int i=0; i<x.size();i++) {
			Document aux = x.get(i);
			
			ArrayList<Document> horas = (ArrayList<Document>) aux.get("gastoHoras");
			
			for (int j = 0; j < horas.size(); j++) {
				Document auxHoras = horas.get(i);
				
				int consumido = auxHoras.getInteger("whconsumo");
				totalconsumido = totalconsumido+ consumido;
				int producido = auxHoras.getInteger("whproducidos");
				totalproducido = totalproducido+ producido;
			}
			break;
		}
		
		System.out.println("Consumido--> " + totalconsumido + "Producido--> " + totalproducido);
		sacarPrecio(totalconsumido,totalproducido, idCliente, mes);
	}


	private void sacarPrecio(int totalconsumido, int totalproducido, ObjectId idCliente,String mes) {

		//70.95;
		//127,80

		
		Bson query1 = Filters.eq("mes",mes);
		Bson query2 = Filters.eq("cliente",idCliente);
		
		MongoClient cn = conexion();
		
		MongoDatabase db = cn.getDatabase("ibertrola");
		
		MongoCollection col = db.getCollection("facturas");
		
		long matchingCount = col.countDocuments(Filters.and(query1,query2));
		
		System.out.println(matchingCount);
		
		if(matchingCount==1) {
			Document fmes = (Document) col.find(Filters.and(query1,query2)).first();
			
			double precioMedioVenta = Double.parseDouble(fmes.getString("precioventa"));
			double precioMedioCompra = Double.parseDouble(fmes.getString("preciocompra"));
			double totalVendido = precioMedioVenta*totalproducido;
			double totalComprado = precioMedioCompra*totalconsumido;
			double factura = totalVendido-totalComprado;
			
			System.out.println("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::");
			System.out.println("\tMES DE " + mes);
			System.out.println("\t WH Producidos " + totalproducido);
			System.out.println("\t WH Consumido " + totalconsumido);
			System.out.println("El precio medio de compra por WH de ese mes fue de " + String.format("%.2f", precioMedioVenta) + "€");
			System.out.println("El precio medio de venta por WH de ese mes fue de " + String.format("%.2f", precioMedioCompra) + "€");
			System.out.println("En total has obtenido " + String.format("%.2f", totalVendido)  + "€ de vender" );
			System.out.println("En total has perdido " + String.format("%.2f", totalComprado)  + "€ de comprar" );
			System.out.println("Debes abonar por este mes " + String.format("%.2f", factura) + "€ a la empresa" );
			System.out.println("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::");
		}else if(matchingCount==0){
			
			double precioMedioVenta = (double)(Math.random()*127.80+70);
			double precioMedioCompra = (double)(Math.random()*40.80+1);
			double totalVendido = precioMedioVenta*totalproducido;
			double totalComprado = precioMedioCompra*totalconsumido;
			double factura = totalVendido-totalComprado;
			
			System.out.println("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::");
			System.out.println("\tMES DE " + mes);
			System.out.println("\t WH Producidos " + totalproducido);
			System.out.println("\t WH Consumido " + totalconsumido);
			System.out.println("El precio medio de compra por WH de ese mes fue de " + String.format("%.2f", precioMedioVenta) + "€");
			System.out.println("El precio medio de venta por WH de ese mes fue de " + String.format("%.2f", precioMedioCompra) + "€");
			System.out.println("En total has obtenido " + String.format("%.2f", totalVendido)  + "€ de vender" );
			System.out.println("En total has perdido " + String.format("%.2f", totalComprado)  + "€ de comprar" );
			System.out.println("Debes abonar por este mes " + String.format("%.2f", factura) + "€ a la empresa" );
			System.out.println("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::");
			
			Document fmes = new Document();
			
			fmes.append("mes", mes);
			fmes.append("cliente", idCliente);
			fmes.append("whproducidos", totalproducido);
			fmes.append("whconsumidos", totalconsumido);
			fmes.append("preciocompra",String.format("%.2f", precioMedioVenta));
			fmes.append("precioventa",String.format("%.2f", precioMedioCompra));
			fmes.append("totalobtenido", String.format("%.2f", totalVendido));
			fmes.append("totalperdido", String.format("%.2f", totalComprado));
			fmes.append("totalabonar", String.format("%.2f", factura));
			
			col.insertOne(fmes);
		}
		
		
	}

	public void eliminarFactura(String c, String m) {
		
		MongoClient cn = conexion();
		
		MongoDatabase db = cn.getDatabase("ibertrola");
		
		MongoCollection col = db.getCollection("clientes");
		
		//
		String ciudad = "";
		
		Bson query = Filters.eq("nombre", c);
		
		Document docCliente = (Document) col.find(query).first();
		
		//____
		ObjectId id = docCliente.getObjectId("_id");
		
		Document docCiudad = (Document) docCliente.get("direccionPrincipal");
		
		ciudad = docCiudad.getString("ciudad");
		
		//Con la ciudad y el mes que nos ha dado podemos buscar en el usuario esa factura
		System.out.println("Ciuadad->" + ciudad + "   Coleccion-> " + mes+ciudad);
		
		String coleccion = mes+ciudad;
		
		ArrayList<Document> facturas = (ArrayList<Document>) docCliente.get("facturas");
		
		for (int i = 0; i < facturas.size(); i++) {
			Document docFactura = facturas.get(i);
			String colAux = docFactura.getString("mes-provincia");
			
			if(coleccion.equals(colAux)) {
				if(eliminarFacturaMes(coleccion,id)) {
					facturas.remove(i);
					
					UpdateResult result  = col.updateOne(query, Updates.set("facturas", facturas));
					System.out.println("Totales actualizados -> " + result.getModifiedCount());
					break;
				}
					
				
			}
			
		}
	
	}

	private boolean eliminarFacturaMes(String coleccion, ObjectId id) {
		try {
		MongoClient cn = conexion();
			
		MongoDatabase db = cn.getDatabase("ibertrola");
			
		MongoCollection col = db.getCollection(coleccion);
	
		Bson query = Filters.eq("ajenaCliente", id);
		
		Document factura = (Document) col.find(query).first();
		
		DeleteResult result = col.deleteOne(factura);
		
		System.out.println("Archivos eliminados -->" + result.getDeletedCount());

		if (result.getDeletedCount()==1)
				return true;
		else
			return false;
		}catch(IllegalArgumentException w) {
			System.out.println("No tiene esa coleccion");
			return false;
		}						
	}

}
