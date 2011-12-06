package com.eucalyptus.auth.crypto;

import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.util.Arrays;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.encoders.UrlBase64;
import com.eucalyptus.component.auth.SystemCredentials;
import com.eucalyptus.crypto.KeyStore;

public class StringCrypto {

	private final String ALIAS = "eucalyptus"; // TODO: don't hardcode these?
	private final String PASSWORD = "eucalyptus";

	private static KeyStore keystore;
	private final String symmetricFormat = "DESede/CBC/PKCS5Padding";
	private String asymmetricFormat = "RSA/ECB/PKCS1Padding";
	private String provider = "BC";

	public static byte [] cat (byte[] bs, byte[] bs2) {
		byte [] result = Arrays.copyOf (bs, bs.length + bs2.length);
		System.arraycopy(bs2, 0, result, bs.length, bs2.length);
		return result;
	}

	public StringCrypto () { }

	public StringCrypto (String format, String provider) 
	{
		this.asymmetricFormat = format;
		this.provider = provider;
		Security.addProvider( new BouncyCastleProvider( ) );
		if (Security.getProvider (this.provider) == null) 
			throw new RuntimeException("cannot find security provider " + this.provider);
		keystore = SystemCredentials.getKeyStore();
		if (keystore==null) 
			throw new RuntimeException ("cannot load keystore or find the key");
	}

	public byte[] encrypt (String password) 
	throws GeneralSecurityException 
	{
		Key pk = keystore.getCertificate(ALIAS).getPublicKey(); 
		Cipher cipher = Cipher.getInstance(this.asymmetricFormat, this.provider);
		cipher.init(Cipher.ENCRYPT_MODE, pk);
		byte [] passwordEncrypted = cipher.doFinal(password.getBytes());
		return UrlBase64.encode(passwordEncrypted);
		//return cat (VMwareBrokerProperties.ENCRYPTION_FORMAT.getBytes(), UrlBase64.encode(passwordEncrypted)); // prepend format
	}

	public String decrypt (String passwordEncoded) 
	throws GeneralSecurityException 
	{
		//String withoutPrefix = passwordEncoded.substring(VMwareBrokerProperties.ENCRYPTION_FORMAT.length(), passwordEncoded.length());
		byte[] passwordEncrypted = UrlBase64.decode(passwordEncoded);
		Key pk = keystore.getKey(ALIAS, PASSWORD);
		Cipher cipher = Cipher.getInstance(this.asymmetricFormat, this.provider);
		cipher.init(Cipher.DECRYPT_MODE, pk);
		return new String(cipher.doFinal(passwordEncrypted));
	}
	
	/**
	 * Decrypt base64 encoded password generated by openssl.
	 * @param passwordEncrypted in base64
	 * @return
	 * @throws GeneralSecurityException
	 */
	public String decryptOpenssl(String passwordEncoded) throws GeneralSecurityException {
	  // Somehow, UrlBase64 in BC can not decode openssl generated base64 string correctly.
	  // We have to use the Base64 from Commons codec library.
	  byte[] passwordEncrypted = Base64.decodeBase64(passwordEncoded.getBytes());
	  Key pk = keystore.getKey(ALIAS, PASSWORD);
	  Cipher cipher = Cipher.getInstance(this.asymmetricFormat, this.provider);
	  cipher.init(Cipher.DECRYPT_MODE, pk);
	  return new String(cipher.doFinal(passwordEncrypted));
	}

	 /**
   * Decrypt base64 encoded password generated by openssl.
   * @param format encryption format
   * @param passwordEncrypted in base64
   * @return
   * @throws GeneralSecurityException
   */
  public String decryptOpenssl(String format, String passwordEncoded) throws GeneralSecurityException {
    // Somehow, UrlBase64 in BC can not decode openssl generated base64 string correctly.
    // We have to use the Base64 from Commons codec library.
    byte[] passwordEncrypted = Base64.decodeBase64(passwordEncoded.getBytes());
    Key pk = keystore.getKey(ALIAS, PASSWORD);
    Cipher cipher = Cipher.getInstance(format, this.provider);
    cipher.init(Cipher.DECRYPT_MODE, pk);
    return new String(cipher.doFinal(passwordEncrypted));
  }

	private byte[] makeKey (String secret) throws NoSuchAlgorithmException 
	{
		// TODO: not sure about all this hanky-panky (this is from stackoverflow.com)
		final MessageDigest md = MessageDigest.getInstance("md5");
		final byte[] digestOfPassword = md.digest(secret.getBytes());
		final byte[] keyBytes = Arrays.copyOf(digestOfPassword, 24);
		for (int j = 0,  k = 16; j < 8;)
		{
			keyBytes[k++] = keyBytes[j++];
		}
		return keyBytes;
	}

	public byte[] encrypt (String string, String secret) 
	throws NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException 
	{		
		final byte[] keyBytes = makeKey(secret);
		final SecretKey key = new SecretKeySpec(keyBytes, "DESede");
		final IvParameterSpec iv = new IvParameterSpec(new byte[8]);
		final Cipher cipher = Cipher.getInstance(this.symmetricFormat);
		cipher.init(Cipher.ENCRYPT_MODE, key, iv);
		final byte[] stringEncrypted = cipher.doFinal(string.getBytes());
		return UrlBase64.encode(stringEncrypted);
	}

	public String decrypt (byte[] stringEncoded, String secret) throws NoSuchAlgorithmException, NoSuchProviderException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, IllegalBlockSizeException, BadPaddingException 
	{
		final byte[] keyBytes = makeKey(secret);
		byte[] stringEncrypted = UrlBase64.decode(stringEncoded);
		final SecretKey key = new SecretKeySpec(keyBytes, "DESede");
		final IvParameterSpec iv = new IvParameterSpec(new byte[8]);
		final Cipher cipher = Cipher.getInstance(this.symmetricFormat);
		cipher.init(Cipher.DECRYPT_MODE, key, iv);
		return new String(cipher.doFinal(stringEncrypted));
	}
}
