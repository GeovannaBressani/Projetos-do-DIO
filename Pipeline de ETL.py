"""
PROJETO ETL - CIÊNCIA DE DADOS
Pipeline completo para processamento de dados financeiros
Autor: Geovanna Gulia Bressani
GitHub: https://github.com/GeovannaBressani
"""

import json
from datetime import datetime
import logging
import sys

# ====================== CONFIGURAÇÃO =======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('etl_pipeline.log', encoding='utf-8', mode='a')
    ]
)
logger = logging.getLogger(__name__)

class PipelineETL:
    """Classe principal do pipeline ETL para finanças pessoais"""
    
    def __init__(self):
        self.metrics = {}
        self.users = []
    
    def extract(self):
        """Fase 1: Extração de dados"""
        logger.info("Fase EXTRACT: Iniciando extração...")
        
        # DADOS SINTÉTICOS (Alternativa 1 do desafio)
        # Simula situação onde a API está fora do ar
        self.users = [
            {"id": 1, "nome": "Ana Silva", "conta": "Corrente", "saldo": 2875.50, "email": "ana@email.com", "idade": 28},
            {"id": 2, "nome": "Bruno Costa", "conta": "Poupança", "saldo": 9250.00, "email": "bruno@email.com", "idade": 35},
            {"id": 3, "nome": "Carla Santos", "conta": "Corrente", "saldo": 320.75, "email": "carla@email.com", "idade": 22},
            {"id": 4, "nome": "Diego Oliveira", "conta": "Investimento", "saldo": 15200.00, "email": "diego@email.com", "idade": 42},
            {"id": 5, "nome": "Elena Rodrigues", "conta": "Corrente", "saldo": 650.00, "email": "elena@email.com", "idade": 31},
            {"id": 6, "nome": "Fabio Lima", "conta": "Poupança", "saldo": 4800.00, "email": "fabio@email.com", "idade": 27},
            {"id": 7, "nome": "Gabriela Souza", "conta": "Universitária", "saldo": 120.25, "email": "gabriela@email.com", "idade": 19}
        ]
        
        logger.info(f"EXTRACT concluído: {len(self.users)} usuários extraídos")
        return self.users
    
    def transform(self):
        """Fase 2: Transformação e enriquecimento dos dados"""
        logger.info("Fase TRANSFORM: Processando dados...")
        
        if not self.users:
            logger.warning("Nenhum dado para transformar")
            return []
        
        timestamp = datetime.now().isoformat()
        
        for user in self.users:
            # 1. Geração de mensagem personalizada (simulação de IA)
            user["mensagem_personalizada"] = self._generate_ai_message(user)
            
            # 2. Categorização financeira
            user["categoria_financeira"] = self._categorize_user(user)
            
            # 3. Recomendações personalizadas
            user["recomendacoes"] = self._generate_recommendations(user)
            
            # 4. Metadados do processamento
            user["processado_em"] = timestamp
            user["versao_pipeline"] = "1.0"
            
            # 5. Alertas inteligentes
            user["alertas"] = self._generate_alerts(user)
        
        # 6. Cálculo de métricas agregadas
        self.metrics = self._calculate_metrics(self.users)
        
        logger.info(f"TRANSFORM concluído: {len(self.users)} usuários transformados")
        return self.users
    
    def _generate_ai_message(self, user):
        """Simula IA gerando mensagem personalizada"""
        nome = user["nome"]
        saldo = user["saldo"]
        conta = user["conta"]
        idade = user.get("idade", 30)
        
        # Sistema de pontuação multifatorial
        score = 0
        
        # Fator 1: Tipo de conta
        if conta == "Investimento":
            score += 3
        elif conta == "Poupança":
            score += 2
        elif conta == "Corrente":
            score += 1
        
        # Fator 2: Saldo
        if saldo > 10000:
            score += 4
        elif saldo > 5000:
            score += 3
        elif saldo > 1000:
            score += 2
        elif saldo > 500:
            score += 1
        
        # Fator 3: Idade (usuários mais jovens têm mais potencial)
        if idade < 25:
            score += 2
        elif idade < 35:
            score += 1
        
        # Mensagens baseadas no score
        if score >= 7:
            return f"🏆 Parabéns, {nome}! Sua saúde financeira é excelente. Saldo: R${saldo:,.2f}"
        elif score >= 5:
            return f"✅ Muito bom, {nome}! Continue assim. Saldo atual: R${saldo:,.2f}"
        elif score >= 3:
            return f"📈 {nome}, você está no caminho certo. Saldo: R${saldo:,.2f}"
        else:
            return f"🎯 {nome}, vamos melhorar juntos sua situação financeira. Saldo: R${saldo:,.2f}"
    
    def _categorize_user(self, user):
        """Categoriza o usuário baseado em seu perfil"""
        saldo = user["saldo"]
        
        if saldo < 500:
            return "Iniciante"
        elif saldo < 3000:
            return "Intermediário"
        elif saldo < 10000:
            return "Avançado"
        else:
            return "Expert"
    
    def _generate_recommendations(self, user):
        """Gera recomendações personalizadas"""
        saldo = user["saldo"]
        conta = user["conta"]
        
        recomendacoes = []
        
        if saldo < 500:
            recomendacoes.append("Criar fundo de emergência")
            recomendacoes.append("Controle de gastos mensais")
        elif saldo < 2000:
            recomendacoes.append("Investir em renda fixa")
            recomendacoes.append("Diversificar aplicações")
        else:
            recomendacoes.append("Investimentos de maior retorno")
            recomendacoes.append("Planejamento patrimonial")
        
        if conta == "Corrente" and saldo > 1000:
            recomendacoes.append("Considerar conta investimento")
        
        return recomendacoes
    
    def _generate_alerts(self, user):
        """Gera alertas baseados no saldo"""
        saldo = user["saldo"]
        
        if saldo < 100:
            return {"nivel": "CRÍTICO", "mensagem": "Saldo muito baixo"}
        elif saldo < 500:
            return {"nivel": "ALTO", "mensagem": "Atenção ao saldo"}
        elif saldo < 1000:
            return {"nivel": "MÉDIO", "mensagem": "Monitore seus gastos"}
        else:
            return {"nivel": "BAIXO", "mensagem": "Situação estável"}
    
    def _calculate_metrics(self, users):
        """Calcula métricas agregadas"""
        if not users:
            return {}
        
        saldos = [u["saldo"] for u in users]
        
        return {
            "total_usuarios": len(users),
            "saldo_total": sum(saldos),
            "saldo_medio": sum(saldos) / len(users),
            "saldo_maximo": max(saldos),
            "saldo_minimo": min(saldos),
            "distribuicao_contas": self._count_account_types(users),
            "faixas_saldo": self._categorize_balances(users)
        }
    
    def _count_account_types(self, users):
        """Conta tipos de conta"""
        contas = {}
        for user in users:
            conta = user["conta"]
            contas[conta] = contas.get(conta, 0) + 1
        return contas
    
    def _categorize_balances(self, users):
        """Categoriza saldos em faixas"""
        faixas = {
            "Abaixo de R$ 500": 0,
            "R$ 500 - R$ 2.000": 0,
            "R$ 2.000 - R$ 5.000": 0,
            "Acima de R$ 5.000": 0
        }
        
        for user in users:
            saldo = user["saldo"]
            if saldo < 500:
                faixas["Abaixo de R$ 500"] += 1
            elif saldo < 2000:
                faixas["R$ 500 - R$ 2.000"] += 1
            elif saldo < 5000:
                faixas["R$ 2.000 - R$ 5.000"] += 1
            else:
                faixas["Acima de R$ 5.000"] += 1
        
        return faixas
    
    def load(self, output_format="all"):
        """Fase 3: Carregamento dos dados processados"""
        logger.info("Fase LOAD: Salvando resultados...")
        
        if not self.users:
            logger.error("Nenhum dado para salvar")
            return False
        
        resultados = []
        
        # Salvar em JSON (para sistemas)
        if output_format in ["json", "all"]:
            if self._save_to_json():
                resultados.append("JSON")
        
        # Salvar relatório (para humanos)
        if output_format in ["txt", "all"]:
            if self._save_report():
                resultados.append("Relatório")
        
        logger.info(f"LOAD concluído: Formatos gerados - {', '.join(resultados)}")
        return len(resultados) > 0
    
    def _save_to_json(self):
        """Salva dados em formato JSON"""
        try:
            data = {
                "metadata": {
                    "processamento": datetime.now().isoformat(),
                    "total_usuarios": len(self.users),
                    "pipeline": "ETL Financeiro v1.0"
                },
                "metricas": self.metrics,
                "usuarios": self.users
            }
            
            with open("output/usuarios_processados.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            return True
        except Exception as e:
            logger.error(f"Erro ao salvar JSON: {e}")
            return False
    
    def _save_report(self):
        """Gera relatório textual"""
        try:
            with open("output/relatorio_financeiro.md", "w", encoding="utf-8") as f:
                f.write("# Relatório Financeiro - Processamento ETL\n\n")
                f.write(f"**Data**: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n")
                f.write(f"**Total de usuários**: {self.metrics['total_usuarios']}\n\n")
                
                f.write("## Métricas Gerais\n")
                f.write(f"- Saldo total: R$ {self.metrics['saldo_total']:,.2f}\n")
                f.write(f"- Saldo médio: R$ {self.metrics['saldo_medio']:,.2f}\n")
                f.write(f"- Saldo máximo: R$ {self.metrics['saldo_maximo']:,.2f}\n")
                f.write(f"- Saldo mínimo: R$ {self.metrics['saldo_minimo']:,.2f}\n\n")
                
                f.write("## Distribuição por Tipo de Conta\n")
                for conta, qtd in self.metrics['distribuicao_contas'].items():
                    f.write(f"- {conta}: {qtd} usuários\n")
                
                f.write("\n## Exemplos de Insights Gerados\n")
                for user in self.users[:3]:  # Mostra 3 exemplos
                    f.write(f"\n### {user['nome']}\n")
                    f.write(f"- Categoria: {user['categoria_financeira']}\n")
                    f.write(f"- Mensagem: {user['mensagem_personalizada']}\n")
                    f.write(f"- Recomendações: {', '.join(user['recomendacoes'])}\n")
                    f.write(f"- Alertas: {user['alertas']['mensagem']} ({user['alertas']['nivel']})\n")
            
            return True
        except Exception as e:
            logger.error(f"Erro ao gerar relatório: {e}")
            return False
    
    def run(self):
        """Executa o pipeline completo"""
        logger.info("🚀 Iniciando pipeline ETL...")
        
        try:
            # EXTRACT
            if not self.extract():
                return False
            
            # TRANSFORM
            if not self.transform():
                return False
            
            # LOAD
            if not self.load():
                return False
            
            # Resultado final
            logger.info("✅ Pipeline executado com sucesso!")
            
            # Mostrar resumo
            self._show_summary()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro no pipeline: {e}")
            return False
    
    def _show_summary(self):
        """Mostra resumo da execução"""
        print("\n" + "="*60)
        print("RESUMO DA EXECUÇÃO - PIPELINE ETL")
        print("="*60)
        print(f"Usuários processados: {len(self.users)}")
        print(f"Saldo total analisado: R$ {self.metrics['saldo_total']:,.2f}")
        print(f"Mensagens personalizadas geradas: {len(self.users)}")
        print(f"\nArquivos gerados:")
        print(f"  • output/usuarios_processados.json")
        print(f"  • output/relatorio_financeiro.md")
        print(f"  • etl_pipeline.log")
        print("="*60)

# ====================== EXECUÇÃO ======================
if __name__ == "__main__":
    # Criar diretório de output
    import os
    os.makedirs("output", exist_ok=True)
    
    # Executar pipeline
    pipeline = PipelineETL()
    success = pipeline.run()
    
    # Código de saída
    sys.exit(0 if success else 1)
