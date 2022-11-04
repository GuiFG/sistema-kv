package sistemakv;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.NoSuchElementException;
import java.util.Scanner;

public class Cliente {

    public static void main(String[] args) throws Exception {
        
        Scanner scanner = new Scanner(System.in);
        
        Boolean run = true;
        while (run) {
           int opcao = menu(scanner); 
           
           switch (opcao) {
               case 1 -> System.out.println("init");
               case 2 -> System.out.println("put");
               case 3 -> System.out.println("get");
               default -> {
                    System.out.println("FIM");
                    run = false;
                    break;
                }
           }
        }
    }
    
    private static int menu(Scanner scanner) {
        System.out.println("1 - INIT");
        System.out.println("2 - PUT");
        System.out.println("3 - GET");
		
        int opcao = 0;
        try {
            opcao = Integer.parseInt(scanner.nextLine());
        }
        catch (NoSuchElementException ex) {}

        return opcao;
    }
    
    private static void enviarMensagem() throws IOException {
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
}



