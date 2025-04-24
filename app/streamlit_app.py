import streamlit as st
import pandas as pd
from sentence_transformers import SentenceTransformer
import os
import numpy as np
import plotly.express as px
from sklearn.decomposition import PCA

# Set page config
st.set_page_config(
    page_title="ML Recommendation System",
   
    layout="wide"
)


@st.cache_resource
def load_model():
    return SentenceTransformer('distiluse-base-multilingual-cased-v2')

model = load_model()

def clean_text(text: str) -> str:
    if pd.isna(text):
        return ""
    return str(text).lower().strip()

def process_dataframe(df: pd.DataFrame):
   
    df['name_shop_clean'] = df['Name Shop'].apply(clean_text)
    df['description_clean'] = df['ABDA Name (Hersteller)'].apply(clean_text)
    
  
    with st.spinner('Generating embeddings...'):
        df['name_shop_embedding'] = df['name_shop_clean'].apply(lambda x: model.encode(x))
        df['description_embedding'] = df['description_clean'].apply(lambda x: model.encode(x))
    
    return df

def visualize_embeddings(embeddings, labels, title):
 
    pca = PCA(n_components=2)
    embeddings_2d = pca.fit_transform(np.vstack(embeddings))
 
    plot_df = pd.DataFrame(embeddings_2d, columns=['PC1', 'PC2'])
    plot_df['Label'] = labels
    
  
    fig = px.scatter(
        plot_df, 
        x='PC1', 
        y='PC2', 
        hover_data=['Label'],
        title=title
    )
    return fig

def main():
    st.title("ML Recommendation System")
    st.write("Process your data and generate embeddings")
    
 
    st.sidebar.title("Options")
    data_source = st.sidebar.radio(
        "Choose Data Source",
        ["Upload CSV", "Use Example Data"]
    )
    
    if data_source == "Upload CSV":
        uploaded_file = st.sidebar.file_uploader("Upload your CSV file", type=['csv'])
        if uploaded_file is not None:
            df = pd.read_csv(uploaded_file)
    else:
      
        data_path = os.path.join("data", "raw", "Feature_Engineering_product_details___AT.csv")
        if os.path.exists(data_path):
            df = pd.read_csv(data_path)
        else:
            st.error("Example data file not found!")
            return
    
    if 'df' in locals():
        st.write("### Data Preview")
        st.write(df.head())
        
        if st.button("Process Data"):
            processed_df = process_dataframe(df)
        
            st.write("### Processed Data")
            st.write("Sample of processed text:")
            st.write(processed_df[['Name Shop', 'name_shop_clean', 'ABDA Name (Hersteller)', 'description_clean']].head())
            
          
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("### Name Shop Embeddings Visualization")
                fig1 = visualize_embeddings(
                    processed_df['name_shop_embedding'].tolist(),
                    processed_df['Name Shop'].tolist(),
                    "Name Shop Embeddings (2D PCA)"
                )
                st.plotly_chart(fig1)
            
            with col2:
                st.write("### Description Embeddings Visualization")
                fig2 = visualize_embeddings(
                    processed_df['description_embedding'].tolist(),
                    processed_df['ABDA Name (Hersteller)'].tolist(),
                    "Description Embeddings (2D PCA)"
                )
                st.plotly_chart(fig2)
            
           
            st.download_button(
                label="Download Processed Data",
                data=processed_df.to_csv(index=False).encode('utf-8'),
                file_name="processed_data.csv",
                mime="text/csv"
            )

if __name__ == "__main__":
    main() 