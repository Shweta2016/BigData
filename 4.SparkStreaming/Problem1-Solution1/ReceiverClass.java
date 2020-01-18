package nu1;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class ReceiverClass extends Receiver<String> {

	int port = -1;
	long count = 0;


	public ReceiverClass(int port_) {
		super(StorageLevel.MEMORY_AND_DISK_2());

		port = port_;
	}

	public void onStart() {
		// Start the thread that receives data over a connection

		new Thread()  {
			@Override public void run() {
				receive();
			}
		}.start();
	}

	public void onStop() {
	}

	/** Create a socket connection and receive data until receiver is stopped */
	private void receive() {
		DatagramSocket socket = null;
		String userInput = null;

		try {
			// connect to the server

			socket = new DatagramSocket(port);
			byte[] receiveData = new byte[1024];
			while(!isStopped()){
				DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
				socket.receive(receivePacket);
				userInput = new String( receivePacket.getData());

				// Until stopped or connection broken continue reading

				store(userInput);
				count++;
			}

			socket.close();

			// Restart in an attempt to connect again when server is active again
			restart("Trying to connect again");

		} catch(ConnectException ce) {
			// restart if could not connect to server
			restart("Could not connect", ce);
		} catch(Throwable t) {
			// restart if there is any other error
			restart("Error receiving data", t);
		}
	}
}