package sistemakv;

import com.google.gson.Gson;
import java.util.HashMap;
import java.util.UUID;


public class Mensagem {
    public static final int PUT = 1;
    public static final int GET = 1;
    
    private UUID id;
    private int tipo;
    private String chave;
    private String valor;
    private String ipPortaDestino;
    private String timestamp;
    
    public Mensagem(String ipPortaDestino, String chave, String valor) {
        this.id = UUID.randomUUID();
        this.ipPortaDestino = ipPortaDestino;   
        this.chave = chave;
        this.valor = valor;
    }
    
    public Mensagem(
            String ipPortaDestino,
            String chave,
            String valor,
            String timestamp
    ) {
        this.id = UUID.randomUUID();
        this.ipPortaDestino = ipPortaDestino;   
        this.chave = chave;
        this.valor = valor;
        this.timestamp = timestamp;
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
    
    public int getTipo() {
        return this.tipo;
    }
    
    public String getChave() {
        return this.chave;
    }
    
    public String getValor() {
        return this.valor;
    }
    
    public String getIpPortaDestino() {
        return this.ipPortaDestino;
    }
    
    public void setIpPortaDestino(String ipPorta) {
        this.ipPortaDestino = ipPorta;
    }
}
