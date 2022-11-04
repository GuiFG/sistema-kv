
package sistemakv;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Servidor {
    private static String ipPorta;
    private static String ipPortaLider;
    
    
    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);

        inicializacao(scanner);
        
        Servidor servidor = new Servidor();
        servidor.criarAtendimento();
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
    
    
    private static void inicializacao(Scanner scanner) {
        System.out.println("IP PORTA = ");
        ipPorta = lerIpPorta(scanner);
        
        System.out.println("IP PORTA LIDER = ");
        ipPortaLider = lerIpPorta(scanner);
    }
    
    private static String lerIpPorta(Scanner scanner) {
        String ipPorta = "";
        while (true) {
            ipPorta = scanner.nextLine();

            String regex = "\\d{3}.+:\\d{1,5}";
            Boolean valido = ehValido(ipPorta, regex);

            if (valido) {
                break;
            }

            System.out.println("Valor nao esta no formato correto. Ex: 127.0.0.1:1234");
        }

        return ipPorta;
    }

    private static Boolean ehValido(String valor, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(valor);

        return matcher.find();
    }
    
    private void criarAtendimento() throws IOException {
        ServerSocket serverSocket = new ServerSocket(1234);
        while (true) {
            System.out.println("Esperando conexao");
            Socket no = serverSocket.accept();
            System.out.println("Conexao aceita");
            
            ThreadAtendimento thread = new ThreadAtendimento(no);
            thread.start();
        }
    }
}