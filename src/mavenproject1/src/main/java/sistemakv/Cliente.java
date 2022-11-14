package sistemakv;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
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

    public class ThreadAtendimento extends Thread {

        private final Socket no;

        public ThreadAtendimento(Socket no) {
            this.no = no;
        }

        @Override
        public void run() {
            try {
                Mensagem mensagem = recuperaMensagemStream();

                processarMensagem(mensagem);
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
        }

        private Mensagem recuperaMensagemStream() throws IOException {
            InputStreamReader is = new InputStreamReader(no.getInputStream());
            BufferedReader reader = new BufferedReader(is);

            System.out.println("Recuperando a mensagem da stream");
            String texto = reader.readLine();

            System.out.println("texto recuperado " + texto);

            Mensagem mensagem = Mensagem.desserializar(texto);

            return mensagem;
        }

        private void processarMensagem(Mensagem mensagem) throws IOException {
            switch (mensagem.getTipo()) {
                case Mensagem.PUT_OK ->
                    putOk(mensagem);
                case Mensagem.GET ->
                    get(mensagem);
                default -> {
                }
            }
        }

        private void putOk(Mensagem resposta) {
            inserirChaveValor(resposta);

            System.out.println("PUT_OK key: " + resposta.getChave() + " value: " + resposta.getValor() + " timestamp "
                    + resposta.getTimestamp() + " realizada no servidor " + resposta.getIpPortaOrigem());
        }

        private void get(Mensagem resposta) {
            if (resposta.getTipo() == Mensagem.TRY_OTHER_SERVER_OR_LATER) {
                System.out.println("GET key: " + resposta.getChave() + " obtido do servidor " + resposta.getIpPortaOrigem()
                        + ", meu timestamp " + resposta.getTimestamp() + " e do servidor " + resposta.getTimestamp()
                        + ". Tentar novamente mais tarde ou em outro servidor.");
                return;
            }

            System.out.println("GET key: " + resposta.getChave() + " value: " + resposta.getValor() + " obtido do servidor " + resposta.getIpPortaOrigem()
                    + ", meu timestamp " + resposta.getTimestamp() + " e do servidor " + resposta.getTimestamp());

            atualizarTimestamp(resposta);
        }
    }

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
            } catch (IOException ex) {
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

    private static void inicializacao(Scanner scanner) throws IOException {
        ipPorta = lerIpPorta(scanner, "IP:PORTA = ");

        for (int i = 0; i < TOTAL_SERVIDORES; i++) {
            String ipServidor = lerIpPorta(scanner, "IP:PORTA SERVIDOR = ");

            ipPortasServidores.add(ipServidor);
        }

        Cliente client = new Cliente();
        client.criarAtendimento();
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

    private void criarAtendimento() throws IOException {
        int porta = recuperaPorta(ipPorta);

        ServerSocket serverSocket = new ServerSocket(porta);
        while (true) {
            System.out.println("Esperando conexao");
            Socket no = serverSocket.accept();
            System.out.println("Conexao aceita");

            ThreadAtendimento thread = new ThreadAtendimento(no);
            thread.start();
        }
    }

    private static void inserirChaveValor(Scanner scanner) throws IOException {
        System.out.println("CHAVE = ");
        String chave = scanner.nextLine();
        System.out.println("VALOR = ");
        String valor = scanner.nextLine();

        String ipServidor = recuperarServidorAleatorio();
        Mensagem mensagem = Mensagem.criarPutClient(ipPorta, ipServidor, chave, valor);
        enviarMensagem(mensagem);
    }

    private void inserirChaveValor(Mensagem mensagem) {
        String chave = mensagem.getChave();
        String valor = mensagem.getValor();

        ArrayList<String> valores = new ArrayList<>();
        valores.add(0, valor);
        valores.add(1, mensagem.getTimestamp());

        tabelaHash.put(chave, valores);
    }

    private static void recuperarValorDaChave(Scanner scanner) throws IOException {
        System.out.println("CHAVE = ");
        String chave = scanner.nextLine();

        if (!tabelaHash.containsKey(chave)) {
            System.out.println("Chave " + chave + " nao existe");
            return;
        }

        String timestamp = tabelaHash.get(chave).get(1);

        System.out.println("Recuperando valor da chave " + chave + " com timestamp " + timestamp);

        String ipServidor = recuperarServidorAleatorio();

        Mensagem mensagem = Mensagem.criarGetClient(ipPorta, ipServidor, chave, timestamp);
        enviarMensagem(mensagem);
    }

    private static void atualizarTimestamp(Mensagem mensagem) {
        ArrayList<String> valores = tabelaHash.get(mensagem.getChave());

        valores.set(1, mensagem.getTimestamp());

        tabelaHash.put(mensagem.getChave(), valores);
    }

    private static void enviarMensagem(Mensagem mensagem) {
        String ipPortaDestino = mensagem.getIpPortaDestino();
        String ip = recuperaIp(ipPortaDestino);
        int porta = recuperaPorta(ipPortaDestino);

        System.out.println("Enviando mensagem para " + ip + " e porta " + porta);

        try ( Socket s = new Socket(ip, porta)) {
            OutputStream os = s.getOutputStream();
            DataOutputStream writer = new DataOutputStream(os);

            String json = Mensagem.serializar(mensagem);
            writer.writeBytes(json + "\n");
            System.out.println("Mensagem enviada");

        } catch (Exception ex) {
            System.out.println("ERRO: " + ex.getMessage());
        }
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
