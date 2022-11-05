
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
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Servidor {
    private static String ipPorta;
    private static String ipPortaLider;
    
    private static final String LOCALHOST = "127.0.0.1";
    private static final int IDX_LIDER = 0;
    private static final String[] SERVIDORES = { "10097", "10098", "10099" };
    
    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);

        inicializacao(scanner);
        
        Servidor servidor = new Servidor();
        servidor.criarAtendimento();
    }
    
    public class ThreadAtendimento extends Thread {
        private final HashMap<String, ArrayList<String>> tabelaHash = new HashMap<>(); 
	private final Socket no;
   
	public ThreadAtendimento(Socket no) {
            this.no = no;
	}
        
	public void run() {
            try {
                Mensagem mensagem = recuperaMensagemStream();
                
                Mensagem resposta = processarMensagem(mensagem);
                
                if (resposta != null)
                    enviarResposta(resposta);
            }
            catch (IOException ex) {
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
        
        private Mensagem processarMensagem(Mensagem mensagem) throws IOException {
            Mensagem resposta = null;
            if (mensagem.getTipo() == Mensagem.PUT) {
                resposta = put(mensagem);
            }
            
            return resposta;
        }
        
        private Mensagem put(Mensagem mensagem) throws IOException {
            if (!igualLider(ipPorta)) {
                String ipLider = recuperaIpLider();
                mensagem.setIpPortaDestino(ipLider);
                
                enviarMensagem(mensagem);
                return null;
            }
          
            String chave = mensagem.getChave();
            String valor = mensagem.getValor();
            ArrayList<String> valores;
            String timestamp;

            if (tabelaHash.containsKey(chave)) {
                valores = tabelaHash.get(chave);
                valores.set(0, valor);
                timestamp = somaString(valores.get(1), 1);
                valores.set(1, timestamp);
            }
            else {
                valores = new ArrayList<>();
                valores.add(0, valor);
                timestamp = "1";
                valores.add(1, timestamp);
            }
            
            tabelaHash.put(chave, valores);
            System.out.println("Enviando mensagens de replicacao");
            ArrayList<Mensagem> mensagens = criarMensagensReplicacao(chave, valor, timestamp);
            
            enviarMensagens(mensagens);
            
            return null;
        }
        
        private Boolean igualLider(String ipPorta) {
            String ipLider = recuperaIpLider();
            return ipLider.equals(ipPorta);
        }
        
        private ArrayList<Mensagem> criarMensagensReplicacao(String chave, String valor, String timestamp) {
            ArrayList<Mensagem> mensagens = new ArrayList<>();
            int total = SERVIDORES.length;
            for (int i = 0; i < total; i++) {
                if (i == IDX_LIDER)
                    continue;
                
                String ipDestino = LOCALHOST + ":" + SERVIDORES[i];
                Mensagem mensagem = new Mensagem(ipDestino, chave, valor, timestamp);
                mensagens.add(mensagem);
            }
            
            return mensagens;
        }
        
        private String somaString(String str1, int valor2) {
            int valor1 = Integer.parseInt(str1);
            
            return String.valueOf(valor1 + valor2);
        }
        
        private void enviarResposta(Mensagem resposta) throws IOException {
            String json = Mensagem.serializar(resposta);
            
            OutputStream os = no.getOutputStream();
            DataOutputStream writer = new DataOutputStream(os);
            writer.writeBytes(json);
        }
        
        private void enviarMensagens(ArrayList<Mensagem> mensagens) throws IOException {
            for (Mensagem mensagem : mensagens) {
                System.out.println("Enviando mensagem para " + mensagem.getIpPortaDestino());
                enviarMensagem(mensagem);
            }
        }
        
        private void enviarMensagem(Mensagem mensagem) throws IOException {
            String json = Mensagem.serializar(mensagem);
            String ipPortaDestino = mensagem.getIpPortaDestino();
            String ip = recuperaIp(ipPortaDestino);
            int porta = recuperaPorta(ipPortaDestino);
            
            Socket s = new Socket(ip, porta);

            OutputStream os = s.getOutputStream();
            DataOutputStream writer = new DataOutputStream(os);

            writer.writeBytes(json);

            s.close();
        }
        
        private static String recuperaIpLider() {
            return LOCALHOST + ":" + SERVIDORES[IDX_LIDER];
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
    
    private static String recuperaIp(String ipPorta) {
        return ipPorta.split(":")[0];
    }

    private static int recuperaPorta(String ipPorta) {
        return Integer.parseInt(ipPorta.split(":")[1]);
    }
}