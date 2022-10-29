package sistemakv;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

public class Cliente {

    public static void main(String[] args) throws Exception {
            Socket s = new Socket("127.0.0.1", 9000);

            OutputStream os = s.getOutputStream();
            DataOutputStream writer = new DataOutputStream(os);

            InputStreamReader is = new InputStreamReader(s.getInputStream());
            BufferedReader reader = new BufferedReader(is);

            BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));

            String texto = inFromUser.readLine();

            writer.writeBytes(texto + "\n");

            String response = reader.readLine();
            System.out.println("DoServidor:" + response);

            s.close();
	}
    
    public class ThreadAtendimento extends Thread {
        private Socket no;

        public ThreadAtendimento(Socket no) {
                this.no = no;
        }

        public void run() {
            try {
                InputStreamReader is = new InputStreamReader(this.no.getInputStream());
                BufferedReader reader = new BufferedReader(is);

                OutputStream os = no.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);

                String texto = reader.readLine();

                writer.writeBytes(texto.toUpperCase() + "\n");
            }
            catch (Exception ex) {
                System.out.println(ex.getMessage());
            }
        }
    }
}



