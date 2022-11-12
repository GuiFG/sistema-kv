package sistemakv;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Cliente {
    
    private static final int INIT = 1;
    private static final int PUT = 2;
    private static final int GET = 3;

    static final int TOTAL_SERVIDORES = 3;
    static final ArrayList<String> ipPortasServidores = new ArrayList<>();
    static final HashMap<String, ArrayList<String>> tabelaHash = new HashMap<>();

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);

        Boolean run = true;
        while (run) {
            int opcao = menu(scanner);

            switch (opcao) {
                case INIT ->
                    inicializacao(scanner);
                case PUT ->
                    inserirChaveValor(scanner);
                case GET ->
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
        System.out.println(INIT + " - INIT");
        System.out.println(PUT + " - PUT");
        System.out.println(GET + " - GET");

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

    private static void inserirChaveValor(Scanner scanner) throws IOException {
        System.out.println("CHAVE = ");
        String chave = scanner.nextLine();
        System.out.println("VALOR = ");
        String valor = scanner.nextLine();

        System.out.println("Enviando chave " + chave + " com valor " + valor);
        
        String ipServidor = recuperarServidorAleatorio();
        Mensagem mensagem = Mensagem.criarPut(ipServidor, chave, valor, null);
        Mensagem resposta = enviarMensagem(mensagem);
        
        if (resposta.getTipo() == Mensagem.PUT_OK)
        {
            ArrayList<String> valores = new ArrayList<>();
            valores.set(0, valor);
            valores.set(1, resposta.getTimestamp());

            tabelaHash.put(chave, valores);
        }
        
        mostrarMensagem(resposta);
    }

    private static void recuperarValorDaChave(Scanner scanner) throws IOException {
        System.out.println("CHAVE = ");
        String chave = scanner.nextLine();
        
        if (!tabelaHash.containsKey(chave))
        {
            System.out.println("Chave nao existe");
            return;
        }
        
        String timestamp = tabelaHash.get(chave).get(1);

        System.out.println("Recuperando valor da chave " + chave + " com timestamp " + timestamp);
       
        String ipServidor = recuperarServidorAleatorio();
        
        Mensagem mensagem = Mensagem.criarGet(ipServidor, chave, timestamp);
        Mensagem resposta = enviarMensagem(mensagem);
        
        mostrarMensagem(resposta);
    }

    private static Mensagem enviarMensagem(Mensagem mensagem) throws IOException {
        String ipPortaDestino = mensagem.getIpPortaDestino();
        String ip = recuperaIp(ipPortaDestino);
        int porta = recuperaPorta(ipPortaDestino);
        
        Socket s = new Socket(ip, porta);

        OutputStream os = s.getOutputStream();
        DataOutputStream writer = new DataOutputStream(os);

        InputStreamReader is = new InputStreamReader(s.getInputStream());
        BufferedReader reader = new BufferedReader(is);
        
        String json = Mensagem.serializar(mensagem);
        writer.writeBytes(json);

        String retorno = reader.readLine();
        Mensagem resposta = Mensagem.desserializar(retorno);
        
        s.close();
        
        return resposta;
    }
    
    private static void mostrarMensagem(Mensagem mensagem) {
        System.out.println(mensagem.getChave() + " " + mensagem.getValor());
    }
    
    private static String recuperaIp(String ipPorta) {
        return ipPorta.split(":")[0];
    }

    private static int recuperaPorta(String ipPorta) {
        return Integer.parseInt(ipPorta.split(":")[1]);
    }
    
    private static String recuperarServidorAleatorio() {
        Random rand = new Random();
        int i = rand.nextInt(TOTAL_SERVIDORES);
        
        return ipPortasServidores.get(i);
    }
}
