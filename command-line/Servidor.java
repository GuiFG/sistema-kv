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
import java.util.Random;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Servidor {

    private static String ipPorta;
    private static String ipPortaLider;

    private static final String LOCALHOST = "127.0.0.1";
    private static int IDX_LIDER = 0;
    private static final String[] SERVIDORES = {"10097", "10098", "10099"};

    private final HashMap<String, ArrayList<String>> tabelaHash = new HashMap<>();
    private final HashMap<String, Integer> contagemReplication = new HashMap<>();

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

        @Override
        public void run() {
            try {
                Mensagem mensagem = recuperaMensagemStream();

                Mensagem resposta = processarMensagem(mensagem);

                if (resposta != null) {
                    enviarMensagem(resposta);
                }
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            } catch (InterruptedException ex) {
                System.out.println(ex.getMessage());
            }
        }

        private Mensagem recuperaMensagemStream() throws IOException {
            InputStreamReader is = new InputStreamReader(no.getInputStream());
            BufferedReader reader = new BufferedReader(is);
            String texto = reader.readLine();

            Mensagem mensagem = Mensagem.desserializar(texto);
            return mensagem;
        }

        private Mensagem processarMensagem(Mensagem mensagem) throws IOException, InterruptedException {
            Mensagem resposta = null;

            switch (mensagem.getTipo()) {
                case Mensagem.PUT:
                    resposta = put(mensagem);
                    break;
                case Mensagem.GET:
                    resposta = get(mensagem);
                    break;
                case Mensagem.REPLICATION:
                    resposta = replication(mensagem);
                    break;
                case Mensagem.REPLICATION_OK:
                    resposta = replicationOk(mensagem);
                    break;
                default:
            }

            return resposta;
        }
        
        // funcao que trata as requisicoes do PUT
        private Mensagem put(Mensagem mensagem) throws IOException, InterruptedException {
            Mensagem resposta;
            
            // caso na seja o lider, encaminha para o lider
            if (!ipPortaLider.equals(ipPorta)) {
                System.out.println("Encaminhando PUT key: " + mensagem.getChave()
                        + " value: " + mensagem.getValor() + ".");
                
                resposta = Mensagem.criarPut(
                        mensagem.getIpPortaOrigem(),
                        ipPortaLider,
                        mensagem.getChave(),
                        mensagem.getValor(),
                        mensagem.getTimestamp()
                );

                return resposta;
            }
            
            // quando for o lider, insere as novas informacoes
            String ipOrigem = mensagem.getIpPortaOrigem();
            System.out.println("Cliente " + ipOrigem + " PUT key: "
                    + mensagem.getChave() + " value: " + mensagem.getValor() + ".");
            String chave = mensagem.getChave();
            String valor = mensagem.getValor();
            String timestamp;
            ArrayList<String> valores;
            
            // Atualiza o timestamp do valor associado a uma determinada chave
            if (tabelaHash.containsKey(chave)) {
                valores = tabelaHash.get(chave);
                valores.set(0, valor);
                timestamp = somaString(valores.get(1), 1);
                valores.set(1, timestamp);
            } else {
                valores = new ArrayList<>();
                valores.add(0, valor);
                timestamp = "1";
                valores.add(1, timestamp);
            }
            
            // atualiza a informacao na tabela hash
            tabelaHash.put(chave, valores);
            
            // envia as mensagens de replicacao para os outros servidores
            ArrayList<Mensagem> mensagens = criarMensagensReplicacao(ipOrigem, chave, valor, timestamp);
            contagemReplication.put(chave, 0);
            enviarMensagens(mensagens);
            
            return null;
        }

        private ArrayList<Mensagem> criarMensagensReplicacao(String ipOrigem, String chave, String valor, String timestamp) {
            ArrayList<Mensagem> mensagens = new ArrayList<>();
            int total = SERVIDORES.length;
            for (int i = 0; i < total; i++) {
                if (i == IDX_LIDER) {
                    continue;
                }

                String ipDestino = LOCALHOST + ":" + SERVIDORES[i];
                Mensagem mensagem = Mensagem.criarReplication(
                        ipOrigem, ipDestino, chave, valor, timestamp
                );
                mensagens.add(mensagem);
            }

            return mensagens;
        }

        private String somaString(String str1, int valor2) {
            int valor1 = Integer.parseInt(str1);

            return String.valueOf(valor1 + valor2);
        }
        
        // funcao que trata as requisicoes do tipo GET
        private Mensagem get(Mensagem mensagem) {
            String log = "Cliente " + mensagem.getIpPortaOrigem() + " key: " + mensagem.getChave()
                    + " ts: " + mensagem.getTimestamp();
            ArrayList<String> valores = tabelaHash.get(mensagem.getChave());

            Mensagem resposta;
            // caso nao tenha na tabela, retorna nulo
            if (valores == null) {
                log += " Chave nao existe.";
                resposta = Mensagem.criarGet(ipPorta, mensagem.getChave(), null, null);

                System.out.println(log);
                return resposta;
            }

            String timestampServer = valores.get(1);
            String timestamp = mensagem.getTimestamp();
            log += ". Meu ts é " + timestampServer + ", portanto devolvendo";

            int cmp = compararTimestamp(timestampServer, timestamp);

            // se o timestamp for menor, tenta novamente
            if (cmp < 0) {
                log += " erro.";
                resposta = Mensagem.criarRetry(ipPorta, mensagem.getChave(), timestampServer);

                System.out.println(log);
                return resposta;
            }

            // timestamp maior ou igual, retorna o valor da chave
            String valor = valores.get(0);
            log += " " + valor + ".";
            resposta = Mensagem.criarGet(ipPorta, mensagem.getChave(), valor, timestampServer);

            System.out.println(log);
            return resposta;
        }

        private int compararTimestamp(String tm1, String tm2) {
            int v1 = Integer.parseInt(tm1);
            int v2 = Integer.parseInt(tm2);

            if (v1 > v2) {
                return 1;
            }

            if (v1 < v2) {
                return -1;
            }

            return 0;
        }
        
        // funcao responsavel pelo tratamento da mensagem REPLICATION enviada pelo servidor lider
        private Mensagem replication(Mensagem mensagem) {
            // Atualiza as informacores recebidas do servidor
            ArrayList<String> valores = new ArrayList<>();
            valores.add(mensagem.getValor());
            valores.add(mensagem.getTimestamp());

            tabelaHash.put(mensagem.getChave(), valores);

            System.out.println("REPLICATION key: " + mensagem.getChave() + " value: " + mensagem.getValor()
                    + " ts: " + mensagem.getTimestamp() + ".");
            
            // retorna a mensagem de REPLICATION_OK para o servidor lider
            Mensagem resposta = Mensagem.criarReplicationOk(mensagem.getIpPortaOrigem(), mensagem.getChave());

            return resposta;
        }
        
        // funcao responsavel pelo tratamento das mensagem de REPLICATION_OK 
        private Mensagem replicationOk(Mensagem mensagem) {
            String chave = mensagem.getChave();
            int total = SERVIDORES.length - 1;
            
            // Verifica se foi atualizado em todos os servidores
            // contagem de mensagem de REPLICATION_OK deve ser igual ao total de servidores vizinhos
            int contagem = contagemReplication.get(chave) + 1;
            if (contagem != total) {
                contagemReplication.put(chave, contagem);
                return null;
            }
            
            // recebido todos os REPLICATION_OK envia a mensagem de PUT_OK para o cliente
            ArrayList<String> valores = tabelaHash.get(chave);
            String valor = valores.get(0);
            String timestamp = valores.get(1);

            String ipPortaCliente = mensagem.getIpPortaOrigem();

            System.out.println("Enviando PUT_OK ao Cliente " + ipPortaCliente + " da key: "
                    + chave + " ts: " + timestamp + ".");

            Mensagem putOk = Mensagem.criarPutOk(ipPorta, ipPortaCliente, chave, valor, timestamp);

            return putOk;
        }

        private void enviarMensagens(ArrayList<Mensagem> mensagens) throws IOException, InterruptedException {
            for (Mensagem mensagem : mensagens) {
                esperarTempoAleatorio();
                enviarMensagem(mensagem);
            }
        }

        private void esperarTempoAleatorio() throws InterruptedException {
            Random random = new Random();
            int n = random.nextInt(20);
            Thread.sleep(n * 1000);
        }

        private void enviarMensagem(Mensagem mensagem) throws IOException {
            switch (mensagem.getModo()) {
                case Mensagem.MODE_SEND:
                    enviar(mensagem);
                    break;
                case Mensagem.MODE_RESPONSE:
                    responder(mensagem);
                    break;
                default:
                    redirecionar(mensagem);
            }
        }

        private void enviar(Mensagem mensagem) throws IOException {
            String ipPortaDestino = mensagem.getIpPortaDestino();
            String ip = recuperaIp(ipPortaDestino);
            int porta = recuperaPorta(ipPortaDestino);

            Mensagem retorno = null;
            try ( Socket s = new Socket(ip, porta)) {
                OutputStream os = s.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);
                InputStreamReader is = new InputStreamReader(s.getInputStream());
                BufferedReader reader = new BufferedReader(is);

                String json = Mensagem.serializar(mensagem);
                writer.writeBytes(json + "\n");

                String texto = reader.readLine();
                Mensagem resposta = Mensagem.desserializar(texto);
                retorno = processarMensagem(resposta);
            } catch (Exception ex) {
                System.out.println("ERRO: " + ex.getMessage());
            }

            if (retorno != null) {
                enviarMensagem(retorno);
            }
        }

        private void responder(Mensagem mensagem) throws IOException {
            OutputStream os = no.getOutputStream();
            DataOutputStream writer = new DataOutputStream(os);

            String json = Mensagem.serializar(mensagem);

            writer.writeBytes(json + "\n");
        }

        private void redirecionar(Mensagem mensagem) throws IOException {
            String json = Mensagem.serializar(mensagem);
            String ipPortaDestino = mensagem.getIpPortaDestino();
            String ip = recuperaIp(ipPortaDestino);
            int porta = recuperaPorta(ipPortaDestino);

            try ( Socket s = new Socket(ip, porta)) {
                OutputStream os = s.getOutputStream();
                DataOutputStream writer = new DataOutputStream(os);
                writer.writeBytes(json + "\n");
            } catch (Exception ex) {
                System.out.println("ERRO: " + ex.getMessage());
            }
        }
    }
    
    // funcao responsavel pela inicializacao do servidor
    private static void inicializacao(Scanner scanner) {
        System.out.println("IP PORTA = ");
        ipPorta = lerIpPorta(scanner);

        System.out.println("IP PORTA LIDER = ");
        ipPortaLider = lerIpPorta(scanner);
        
        String porta = String.valueOf(recuperaPorta(ipPortaLider));
        atualizarIndiceLider(porta);
    }

    private static void atualizarIndiceLider(String porta) {
        int idx = 0;
        for (int i = 0; i < SERVIDORES.length; i++) {
            if (SERVIDORES[i].equals(porta)) {
                idx = i;
                break;
            }
        }

        IDX_LIDER = idx;
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
    
    // funcao que cria as thread de atendimento recebidas do cliente
    private void criarAtendimento() throws IOException {
        int porta = recuperaPorta(ipPorta);

        ServerSocket serverSocket = new ServerSocket(porta);
        while (true) {
            Socket no = serverSocket.accept();
            
            // cria uma nova thread para conexao aceita com o cliente
            ThreadAtendimento thread = new ThreadAtendimento(no);
            thread.start();
        }
    }

    private static String recuperaIp(String ipPorta) {
        return ipPorta.split(":")[0];
    }

    private static int recuperaPorta(String ipPorta) {
        return Integer.parseInt(ipPorta.split(":")[1]);
    }
}
