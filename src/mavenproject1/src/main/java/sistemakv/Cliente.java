package sistemakv;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Cliente {

    static final int TOTAL_SERVIDORES = 3;
    static final ArrayList<String> ipPortasServidores = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);

        Boolean run = true;
        while (run) {
            int opcao = menu(scanner);

            switch (opcao) {
                case 1 ->
                    inicializacao(scanner);
                case 2 ->
                    inserirChaveValor(scanner);
                case 3 ->
                    recuperarValorDaChave(scanner);
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
        } catch (NoSuchElementException ex) {
        }

        return opcao;
    }

    private static void inicializacao(Scanner scanner) {
        for (int i = 0; i < TOTAL_SERVIDORES; i++) {
            String ipPorta = lerIpPorta(scanner);

            ipPortasServidores.add(ipPorta);
        }
    }

    private static String lerIpPorta(Scanner scanner) {
        String ipPorta = "";
        while (true) {
            System.out.println("IP:PORTA = ");
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

    private static void inserirChaveValor(Scanner scanner) {
        System.out.println("CHAVE = ");
        String chave = scanner.nextLine();
        System.out.println("VALOR = ");
        String valor = scanner.nextLine();

        System.out.println("Enviando chave " + chave + " com valor " + valor);

        // escolher servidor de forma aleatoria
        // receber mensagem PUT_OK com timestamp
        // salvar timestamp no hash
    }

    private static void recuperarValorDaChave(Scanner scanner) {
        System.out.println("CHAVE = ");
        String chave = scanner.nextLine();

        System.out.println("Recuperando valor da cahve " + chave);

        // escolher o servidor de forma aleatoria 
        // recupera o timestamp da chave 
        // envia a requisicao do get 
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
