package sistemakv;

import com.google.gson.Gson;
import java.util.HashMap;

public class Mensagem {

    public static final int PUT = 1;
    public static final int PUT_OK = 1;
    public static final int GET = 3;
    public static final int REPLICATION = 4;
    public static final int REPLICATION_OK = 5;
    public static final int TRY_OTHER_SERVER_OR_LATER = 6;
    
    public static final int MODE_SEND = 1;
    public static final int MODE_RESPONSE = 2;
    public static final int MODE_REDIRECT = 3;
    
    private int modo;
    private int tipo;
    private String chave;
    private String valor;
    private String ipPortaOrigem;
    private String ipPortaDestino;
    private String timestamp;

    public Mensagem() {
    }
    
    public static String serializar(Mensagem mensagem) {
        Gson gson = new Gson();
        String json = gson.toJson(mensagem);

        return json;
    }

    public static Mensagem desserializar(String json) {
        Gson gson = new Gson();
        Mensagem mensagem = gson.fromJson(json, Mensagem.class);
        return mensagem;
    }
    
    public static Mensagem criarPut(String ipPortaDestino, String chave, String valor, String timestamp) {
        Mensagem mensagem = new Mensagem();
        
        mensagem.setModo(Mensagem.MODE_REDIRECT);
        mensagem.setTipo(Mensagem.PUT);
        mensagem.setIpPortaDestino(ipPortaDestino);
        mensagem.setChave(chave);
        mensagem.setValor(valor);
        mensagem.setTimestamp(timestamp);
        
        return mensagem;
   }
    
    public static Mensagem criarPutOk(String ipPortaDestino, String chave, String valor, String timestamp) {
        Mensagem mensagem = new Mensagem();
        
        mensagem.setModo(Mensagem.MODE_REDIRECT);
        mensagem.setTipo(Mensagem.PUT_OK);
        mensagem.setIpPortaDestino(ipPortaDestino);
        mensagem.setChave(chave);
        mensagem.setValor(valor);
        mensagem.setTimestamp(timestamp);
        
        return mensagem;
   }

    public static Mensagem criarReplication(
            String ipDestino,
            String chave,
            String valor,
            String timestamp
    ) {
        Mensagem mensagem = new Mensagem();
        
        mensagem.setModo(MODE_SEND);
        mensagem.setTipo(Mensagem.REPLICATION);
        mensagem.setIpPortaDestino(ipDestino);
        mensagem.setChave(chave);
        mensagem.setValor(valor);
        mensagem.setTimestamp(timestamp);

        return mensagem;
    }
    
    public static Mensagem criarReplicationOk() {
        Mensagem mensagem = new Mensagem();
        
        mensagem.setModo(MODE_RESPONSE);
        mensagem.setTipo(Mensagem.REPLICATION_OK);
        
        return mensagem;
    }
    
    public static Mensagem criarGet(String chave, String valor, String timestamp) {
        Mensagem mensagem = new Mensagem();
        
        mensagem.setModo(Mensagem.MODE_RESPONSE);
        mensagem.setTipo(Mensagem.GET);
        mensagem.setChave(chave);
        mensagem.setValor(valor);
        mensagem.setTimestamp(timestamp);
        
        return mensagem;
    }
    
    public static Mensagem criarRetry() {
        Mensagem mensagem = new Mensagem();
        
        mensagem.setModo(Mensagem.MODE_RESPONSE);
        mensagem.setTipo(Mensagem.TRY_OTHER_SERVER_OR_LATER);
        
        return mensagem;
    }
    
    public int getModo() {
        return this.modo;
    }

    public void setModo(int modo) {
        this.modo = modo;
    }

    public int getTipo() {
        return this.tipo;
    }

    public void setTipo(int tipo) {
        this.tipo = tipo;
    }

    public String getChave() {
        return this.chave;
    }

    public void setChave(String chave) {
        this.chave = chave;
    }

    public String getValor() {
        return this.valor;
    }

    public void setValor(String valor) {
        this.valor = valor;
    }

    public String getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getIpPortaOrigem() {
        return this.ipPortaOrigem;
    }

    public void setIpPortaOrigem(String ipPortaOrigem) {
        this.ipPortaOrigem = ipPortaOrigem;
    }

    public String getIpPortaDestino() {
        return this.ipPortaDestino;
    }

    public void setIpPortaDestino(String ipPorta) {
        this.ipPortaDestino = ipPorta;
    }
}
