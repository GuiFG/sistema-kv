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

        private Socket socket;
        private final Mensagem mensagem;

        public ThreadAtendimento(Mensagem mensagem) {
            this.mensagem = mensagem;
            this.socket = null;
        }

        @Override
        public void run() {
            try {
                switch (this.mensagem.getModo()) {
                    case Mensagem.MODE_SEND:
                        enviar(mensagem);
                        break;
                    case Mensagem.MODE_SEND_LISTEN:
                        enviarReceber(mensagem);
                        break;
                    default:
                }

            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
        }

        
        // funcao que envia de fato a mensagem GET para o servidor
        private void enviar(Mensagem mensagem) throws IOException {
            // enviar a mensagem para o servidor
            enviarMensagem(mensagem);
            
            // recupera a resposta usando o mesmo socket anterior
            Mensagem resposta = recuperaMensagemStream();
            
            // atualiza a informacao do GET
            processarMensagem(resposta);
        }
        
        
        // funcao que realiza o envio da mensagem PUT para o servidor
        private void enviarReceber(Mensagem msg) throws IOException {
            // envia a mensagem para o servidor escolhido
            enviarApenas(msg);
               
            // espera a resposta do servidor LIDER com o PUT_OK
            Mensagem resposta = escutarMensagemStream();
            
            // atualiza a informacao recebida
            processarMensagem(resposta);
        }

        private void enviarMensagem(Mensagem mensagem) throws IOException {
            String ipPortaDestino = mensagem.getIpPortaDestino();
            String ip = recuperaIp(ipPortaDestino);
            int porta = recuperaPorta(ipPortaDestino);
            
            this.socket = new Socket(ip, porta);
            OutputStream os = this.socket.getOutputStream();
            DataOutputStream writer = new DataOutputStream(os);

            String json = Mensagem.serializar(mensagem);
            writer.writeBytes(json + "\n");
        }

        private void enviarApenas(Mensagem msg) throws IOException {
            String ipPortaDestino = msg.getIpPortaDestino();
            String ip = recuperaIp(ipPortaDestino);
            int porta = recuperaPorta(ipPortaDestino);
            
            try ( Socket skt = new Socket(ip, porta)) {
                OutputStream os = skt.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);

                String json = Mensagem.serializar(msg);
                writer.writeBytes(json + "\n");
            }
        }

        private Mensagem recuperaMensagemStream() throws IOException {
            InputStreamReader is = new InputStreamReader(this.socket.getInputStream());
            BufferedReader reader = new BufferedReader(is);
            String texto = reader.readLine();
            Mensagem resposta = Mensagem.desserializar(texto);

            return resposta;
        }

        private Mensagem escutarMensagemStream() throws IOException {
            int porta = recuperaPorta(ipPorta);

            Mensagem resposta;
            try (ServerSocket serverSocket = new ServerSocket(porta)) {
                try (Socket no = serverSocket.accept()) {
                    InputStreamReader is = new InputStreamReader(no.getInputStream());
                    BufferedReader reader = new BufferedReader(is);
                    String texto = reader.readLine();
                    resposta = Mensagem.desserializar(texto);
                }
            }

            return resposta;
        }

        private void processarMensagem(Mensagem mensagem) throws IOException {
            switch (mensagem.getTipo()) {
                case Mensagem.PUT_OK:
                    putOk(mensagem);
                    break;
                case Mensagem.GET:
                    get(mensagem);
                    break;
                case Mensagem.TRY_OTHER_SERVER_OR_LATER:
                    tryOtherServerOrLater(mensagem);
                    break;
                default:
            }
        }

        private void putOk(Mensagem resposta) {
            inserirChaveValor(resposta);

            System.out.println("PUT_OK key: " + resposta.getChave() + " value: " + resposta.getValor() + " timestamp "
                    + resposta.getTimestamp() + " realizada no servidor " + resposta.getIpPortaOrigem());
        }

        private void get(Mensagem resposta) {
            String timestamp = recuperaTimestamp(resposta);

            System.out.println("GET key: " + resposta.getChave() + " value: " + resposta.getValor() + " obtido do servidor " + resposta.getIpPortaOrigem()
                    + ", meu timestamp " + timestamp + " e do servidor " + resposta.getTimestamp());

            atualizarTimestamp(resposta);
        }

        private void tryOtherServerOrLater(Mensagem resposta) {
            String timestamp = recuperaTimestamp(resposta);

            System.out.println("GET key: " + resposta.getChave() + " obtido do servidor " + resposta.getIpPortaOrigem()
                    + ", meu timestamp " + timestamp + " e do servidor " + resposta.getTimestamp()
                    + ". Tentar novamente mais tarde ou em outro servidor.");
        }
    }

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);

        Boolean run = true;
        Cliente cliente = new Cliente();
        while (run) {
            try {
                int opcao = menu(scanner);

                switch (opcao) {
                    case INIT:
                        inicializacao(scanner);
                        break;
                    case PUT:
                        cliente.inserirChaveValor(scanner);
                        break;
                    case GET:
                        cliente.recuperarValorDaChave(scanner);
                        break;
                    default:
                        System.out.println("FIM");
                        run = false;
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
    
    // funcao responsavel pela inicializacao do cliente
    private static void inicializacao(Scanner scanner) throws IOException {
        // le o ip e porta do client atual
        ipPorta = lerIpPorta(scanner, "IP:PORTA = ");

        // le o ip e porta dos servidores
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
    
    // funcao recupera do teclado a chave e o valor para realizar o PUT
    private void inserirChaveValor(Scanner scanner) throws IOException {
        System.out.println("CHAVE = ");
        String chave = scanner.nextLine();

        System.out.println("VALOR = ");
        String valor = scanner.nextLine();

        inserirChaveValor(chave, valor);
        
        // escolhe o servidor de forma aleatoria 
        String ipServidor = recuperarServidorAleatorio();
        
        
        Mensagem mensagem = Mensagem.criarPutClient(ipPorta, ipServidor, chave, valor);
        
        // criar a thread para realizar o envio da mensagem
        ThreadAtendimento thread = new ThreadAtendimento(mensagem);
        thread.start();
    }

    private void inserirChaveValor(Mensagem mensagem) {
        String chave = mensagem.getChave();
        String valor = mensagem.getValor();

        ArrayList<String> valores = new ArrayList<>();
        valores.add(0, valor);
        valores.add(1, mensagem.getTimestamp());

        tabelaHash.put(chave, valores);
    }

    private static void inserirChaveValor(String chave, String valor) {
        String timestamp = "0";
        if (tabelaHash.containsKey(chave)) {
            timestamp = tabelaHash.get(chave).get(1);
        }
        timestamp = somaString(timestamp, 1);

        ArrayList valores = new ArrayList<>();
        valores.add(0, valor);
        valores.add(1, timestamp);

        tabelaHash.put(chave, valores);
    }

    private static String somaString(String str1, int valor2) {
        int valor1 = Integer.parseInt(str1);

        return String.valueOf(valor1 + valor2);
    }
    
    
    // funcao que recupera do teclado a chave a ser usada no GET
    private void recuperarValorDaChave(Scanner scanner) throws IOException {
        System.out.println("CHAVE = ");
        String chave = scanner.nextLine();
        
        // caso a chave ainda nao foi registrada, determina o timestamp dela como zero
        String timestamp = "0";
        if (tabelaHash.containsKey(chave)) {
            timestamp = tabelaHash.get(chave).get(1);
        }
        
        // escolhe o servidor de forma aleatoria
        String ipServidor = recuperarServidorAleatorio();

        Mensagem mensagem = Mensagem.criarGetClient(ipPorta, ipServidor, chave, timestamp);
        
        // cria a thread para realizar a requiscao GET no servidor
        ThreadAtendimento thread = new ThreadAtendimento(mensagem);
        thread.start();
    }

    private static void atualizarTimestamp(Mensagem mensagem) {
        String chave = mensagem.getChave();
        if (!tabelaHash.containsKey(chave)) {
            ArrayList<String> valores = new ArrayList<>();
            valores.add(mensagem.getValor());
            valores.add(mensagem.getTimestamp());
            
            tabelaHash.put(chave, valores);
            return;
        }
        
        ArrayList<String> valores = tabelaHash.get(chave);

        valores.set(1, mensagem.getTimestamp());

        tabelaHash.put(chave, valores);
    }

    private String recuperaTimestamp(Mensagem msg) {
        String chave = msg.getChave();
        
        if (tabelaHash.containsKey(chave))
            return tabelaHash.get(chave).get(1);
            
        return "0";
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
