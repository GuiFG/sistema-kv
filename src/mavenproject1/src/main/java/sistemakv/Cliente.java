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
    static String ipPorta;

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);

        Boolean run = true;
        while (run) {
            try {
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
            catch (Exception ex) {
                System.out.println("ERRO: " + ex.getMessage());
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
        ipPorta = lerIpPorta(scanner, "IP:PORTA = ");
        
        for (int i = 0; i < TOTAL_SERVIDORES; i++) {
            String ipServidor = lerIpPorta(scanner, "IP:PORTA SERVIDOR = ");

            ipPortasServidores.add(ipServidor);
        }
    }

    private static String lerIpPorta(Scanner scanner, String mensagemInput) {
        String ipPorta = "";
        while (true) {
            System.out.println(mensagemInput);
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

        String ipServidor = recuperarServidorAleatorio();
        Mensagem mensagem = Mensagem.criarPutClient(ipPorta, ipServidor, chave, valor);
        Mensagem resposta = enviarMensagem(mensagem);
        
        if (resposta == null)
            return;
        
        if (resposta.getTipo() != Mensagem.PUT_OK)
        {
            System.out.println("PUT_OK key: " + resposta.getChave() + " value: " + resposta.getValor() + " timestamp " + 
                resposta.getTimestamp() + " realizada no servidor " + resposta.getIpPortaOrigem()
                + ". Erro ao inserir a chave");
            return;
        }
        
        ArrayList<String> valores = new ArrayList<>();
        valores.add(0, valor);
        valores.add(1, resposta.getTimestamp());

        tabelaHash.put(chave, valores);
        
        System.out.println("PUT_OK key: " + resposta.getChave() + " value: " + resposta.getValor() + " timestamp " + 
                resposta.getTimestamp() + " realizada no servidor " + resposta.getIpPortaOrigem());
    }

    private static void recuperarValorDaChave(Scanner scanner) throws IOException {
        System.out.println("CHAVE = ");
        String chave = scanner.nextLine();
        
        if (!tabelaHash.containsKey(chave))
        {
            System.out.println("Chave " + chave + " nao existe");
            return;
        }
        
        String timestamp = tabelaHash.get(chave).get(1);

        System.out.println("Recuperando valor da chave " + chave + " com timestamp " + timestamp);
       
        String ipServidor = recuperarServidorAleatorio();
        
        Mensagem mensagem = Mensagem.criarGetClient(ipPorta, ipServidor, chave, timestamp);
        Mensagem resposta = enviarMensagem(mensagem);
        
        if (resposta == null)
        {
            System.out.println("Erro ao enviar a mensagem");
            return;
        }
        
        if (resposta.getTipo() == Mensagem.TRY_OTHER_SERVER_OR_LATER)
        {
            System.out.println("GET key: " + resposta.getChave() + " obtido do servidor " + resposta.getIpPortaOrigem() 
                + ", meu timestamp " + timestamp + " e do servidor " + resposta.getTimestamp() 
                + ". Tentar novamente mais tarde ou em outro servidor.");
            return;
        }
        
        System.out.println("GET key: " + resposta.getChave() + " value: " + resposta.getValor() + " obtido do servidor " + resposta.getIpPortaOrigem() 
                + ", meu timestamp " + timestamp + " e do servidor " + resposta.getTimestamp());
    }

    private static Mensagem enviarMensagem(Mensagem mensagem) throws IOException {
        String ipPortaDestino = mensagem.getIpPortaDestino();
        String ip = recuperaIp(ipPortaDestino);
        int porta = recuperaPorta(ipPortaDestino);
        
        System.out.println("Enviando mensagem para " + ip + " e porta " + porta);
        
        Mensagem resposta = null;
        try (Socket s = new Socket(ip, porta)) {
            OutputStream os = s.getOutputStream();
            DataOutputStream writer = new DataOutputStream(os);
            InputStreamReader is = new InputStreamReader(s.getInputStream());
            BufferedReader reader = new BufferedReader(is);
            
            String json = Mensagem.serializar(mensagem);
            writer.writeBytes(json + "\n");
            System.out.println("mensagem enviada, esperando resposta do servidor");
            String retorno = reader.readLine();
            resposta = Mensagem.desserializar(retorno);
        } catch (Exception ex) {
            System.out.println("ERRO: " + ex.getMessage());
        }
        
        return resposta;
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
