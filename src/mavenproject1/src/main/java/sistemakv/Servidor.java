
package sistemakv;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;




public class Servidor {
    
    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(9000);
        while (true) {
            System.out.println("Esperando conexao");
            Socket no = serverSocket.accept();
            System.out.println("Conexao aceita");
            
            Servidor servidor = new Servidor();
            servidor.criarAtendimento(no);
        }
    }
    
    public class ThreadAtendimento extends Thread {
	private final Socket no;
   
	public ThreadAtendimento(Socket no) {
            this.no = no;
	}
        
	public void run() {
            try {
                InputStreamReader is = new InputStreamReader(no.getInputStream());
                BufferedReader reader = new BufferedReader(is);

                OutputStream os = no.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);

                String texto = reader.readLine();

                writer.writeBytes(texto.toUpperCase() + "\n");
            }
            catch (IOException ex) {
                    System.out.println(ex.getMessage());
            }
	}
    }
    
    private void criarAtendimento(Socket socket) {
        ThreadAtendimento thread = new ThreadAtendimento(socket);
        thread.start();
    }
}